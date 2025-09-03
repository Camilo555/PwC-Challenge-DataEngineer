"""
API Service Manager
Centralized service layer for business logic separation and dependency injection.
"""

from __future__ import annotations

import asyncio
import traceback
from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Generic, TypeVar

from pydantic import BaseModel, Field

from api.services.service_locator import get_service_locator, register_singleton
from core.config import get_settings
from core.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()

T = TypeVar("T")


class ServiceStatus(str, Enum):
    """Service status enumeration"""

    INITIALIZING = "initializing"
    RUNNING = "running"
    ERROR = "error"
    STOPPED = "stopped"
    MAINTENANCE = "maintenance"


class ServicePriority(int, Enum):
    """Service priority levels"""

    CRITICAL = 1
    HIGH = 2
    NORMAL = 3
    LOW = 4
    BACKGROUND = 5


@dataclass
class ServiceMetrics:
    """Service performance and health metrics"""

    name: str
    status: ServiceStatus
    start_time: datetime
    last_health_check: datetime
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    average_response_time: float = 0.0
    memory_usage_mb: float = 0.0
    cpu_usage_percent: float = 0.0
    error_rate: float = 0.0
    last_error: str | None = None
    dependencies: list[str] = field(default_factory=list)

    @property
    def success_rate(self) -> float:
        """Calculate service success rate"""
        if self.total_requests == 0:
            return 1.0
        return self.successful_requests / self.total_requests

    @property
    def uptime_seconds(self) -> float:
        """Calculate service uptime in seconds"""
        return (datetime.utcnow() - self.start_time).total_seconds()

    def to_dict(self) -> dict[str, Any]:
        """Convert metrics to dictionary"""
        return {
            "name": self.name,
            "status": self.status.value,
            "start_time": self.start_time.isoformat(),
            "last_health_check": self.last_health_check.isoformat(),
            "total_requests": self.total_requests,
            "successful_requests": self.successful_requests,
            "failed_requests": self.failed_requests,
            "success_rate": self.success_rate,
            "error_rate": self.error_rate,
            "average_response_time_ms": self.average_response_time * 1000,
            "uptime_seconds": self.uptime_seconds,
            "memory_usage_mb": self.memory_usage_mb,
            "cpu_usage_percent": self.cpu_usage_percent,
            "last_error": self.last_error,
            "dependencies": self.dependencies,
        }


class ServiceResult(BaseModel, Generic[T]):
    """Standard service operation result"""

    success: bool
    data: T | None = None
    error: str | None = None
    error_code: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    execution_time_ms: float | None = None

    @classmethod
    def success_result(
        cls, data: T, metadata: dict[str, Any] = None, execution_time_ms: float = None
    ) -> ServiceResult[T]:
        """Create successful service result"""
        return cls(
            success=True, data=data, metadata=metadata or {}, execution_time_ms=execution_time_ms
        )

    @classmethod
    def error_result(
        cls, error: str, error_code: str = None, metadata: dict[str, Any] = None
    ) -> ServiceResult[T]:
        """Create error service result"""
        return cls(success=False, error=error, error_code=error_code, metadata=metadata or {})


class BaseAPIService(ABC):
    """
    Base class for all API services.
    Provides common functionality for service lifecycle, health checks, and metrics.
    """

    def __init__(self, name: str, priority: ServicePriority = ServicePriority.NORMAL):
        self.name = name
        self.priority = priority
        self.metrics = ServiceMetrics(
            name=name,
            status=ServiceStatus.INITIALIZING,
            start_time=datetime.utcnow(),
            last_health_check=datetime.utcnow(),
        )
        self._is_initialized = False
        self._dependencies: list[str] = []
        self._health_check_interval = 60  # seconds
        self._last_health_check = datetime.utcnow()

        logger.info(f"Initializing service: {self.name}")

    async def initialize(self) -> None:
        """Initialize the service"""
        try:
            self.metrics.status = ServiceStatus.INITIALIZING
            await self._initialize()
            self._is_initialized = True
            self.metrics.status = ServiceStatus.RUNNING
            logger.info(f"Service {self.name} initialized successfully")
        except Exception as e:
            self.metrics.status = ServiceStatus.ERROR
            self.metrics.last_error = str(e)
            logger.error(f"Failed to initialize service {self.name}: {e}")
            raise

    async def shutdown(self) -> None:
        """Shutdown the service"""
        try:
            self.metrics.status = ServiceStatus.STOPPED
            await self._shutdown()
            logger.info(f"Service {self.name} shutdown successfully")
        except Exception as e:
            self.metrics.last_error = str(e)
            logger.error(f"Error during service {self.name} shutdown: {e}")
            raise

    async def health_check(self) -> bool:
        """Perform health check"""
        try:
            if not await self._health_check():
                self.metrics.status = ServiceStatus.ERROR
                return False

            self.metrics.status = ServiceStatus.RUNNING
            self.metrics.last_health_check = datetime.utcnow()
            return True
        except Exception as e:
            self.metrics.status = ServiceStatus.ERROR
            self.metrics.last_error = str(e)
            logger.error(f"Health check failed for service {self.name}: {e}")
            return False

    @asynccontextmanager
    async def track_operation(self, operation_name: str = None) -> AsyncGenerator[None, None]:
        """Context manager to track service operations"""
        start_time = datetime.utcnow()
        self.metrics.total_requests += 1

        try:
            yield
            # Operation succeeded
            self.metrics.successful_requests += 1
            execution_time = (datetime.utcnow() - start_time).total_seconds()

            # Update average response time (exponential moving average)
            if self.metrics.average_response_time == 0:
                self.metrics.average_response_time = execution_time
            else:
                alpha = 0.1  # Smoothing factor
                self.metrics.average_response_time = (
                    alpha * execution_time + (1 - alpha) * self.metrics.average_response_time
                )

        except Exception as e:
            # Operation failed
            self.metrics.failed_requests += 1
            self.metrics.last_error = str(e)
            self.metrics.error_rate = self.metrics.failed_requests / self.metrics.total_requests

            logger.error(
                f"Operation failed in service {self.name}",
                extra={
                    "service": self.name,
                    "operation": operation_name or "unknown",
                    "error": str(e),
                    "traceback": traceback.format_exc(),
                },
            )
            raise

    def get_metrics(self) -> ServiceMetrics:
        """Get current service metrics"""
        return self.metrics

    def add_dependency(self, service_name: str) -> None:
        """Add service dependency"""
        if service_name not in self._dependencies:
            self._dependencies.append(service_name)
            self.metrics.dependencies = self._dependencies.copy()

    async def check_dependencies(self) -> bool:
        """Check if all dependencies are healthy"""
        service_locator = get_service_locator()

        for dep_name in self._dependencies:
            dep_service = service_locator.get_service(dep_name)
            if not dep_service:
                logger.warning(f"Dependency {dep_name} not found for service {self.name}")
                return False

            if hasattr(dep_service, "health_check"):
                if not await dep_service.health_check():
                    logger.warning(
                        f"Dependency {dep_name} health check failed for service {self.name}"
                    )
                    return False

        return True

    # Abstract methods to be implemented by concrete services
    @abstractmethod
    async def _initialize(self) -> None:
        """Service-specific initialization logic"""
        pass

    @abstractmethod
    async def _shutdown(self) -> None:
        """Service-specific shutdown logic"""
        pass

    @abstractmethod
    async def _health_check(self) -> bool:
        """Service-specific health check logic"""
        pass


class SalesAnalyticsService(BaseAPIService):
    """Service for sales analytics and reporting"""

    def __init__(self):
        super().__init__("sales_analytics", ServicePriority.HIGH)
        self._cache = {}
        self._cache_ttl = timedelta(minutes=15)

    async def _initialize(self) -> None:
        """Initialize sales analytics service"""
        # Initialize database connections, cache, etc.
        self.add_dependency("database_service")
        self.add_dependency("cache_service")
        logger.info("Sales Analytics Service initialized")

    async def _shutdown(self) -> None:
        """Shutdown sales analytics service"""
        self._cache.clear()
        logger.info("Sales Analytics Service shutdown")

    async def _health_check(self) -> bool:
        """Health check for sales analytics service"""
        # Check database connectivity, cache availability, etc.
        return await self.check_dependencies()

    async def get_sales_summary(
        self, start_date: datetime, end_date: datetime, filters: dict[str, Any] = None
    ) -> ServiceResult[dict[str, Any]]:
        """Get sales summary for date range"""
        async with self.track_operation("get_sales_summary"):
            try:
                # Check cache first
                cache_key = (
                    f"sales_summary:{start_date.date()}:{end_date.date()}:{hash(str(filters))}"
                )

                if cache_key in self._cache:
                    cache_entry = self._cache[cache_key]
                    if datetime.utcnow() - cache_entry["timestamp"] < self._cache_ttl:
                        return ServiceResult.success_result(
                            cache_entry["data"], metadata={"cached": True}
                        )

                # Simulate business logic
                await asyncio.sleep(0.1)  # Simulate processing time

                summary_data = {
                    "total_sales": 1250000.50,
                    "total_orders": 4567,
                    "average_order_value": 273.82,
                    "top_products": [
                        {"name": "Product A", "sales": 125000},
                        {"name": "Product B", "sales": 98000},
                    ],
                    "sales_by_region": {"North America": 450000, "Europe": 380000, "Asia": 420000},
                    "period": {
                        "start_date": start_date.isoformat(),
                        "end_date": end_date.isoformat(),
                    },
                }

                # Apply filters if provided
                if filters:
                    summary_data["applied_filters"] = filters

                # Cache the result
                self._cache[cache_key] = {"data": summary_data, "timestamp": datetime.utcnow()}

                return ServiceResult.success_result(
                    summary_data, metadata={"cached": False, "processing_time_ms": 100}
                )

            except Exception as e:
                return ServiceResult.error_result(
                    error=f"Failed to get sales summary: {str(e)}", error_code="SALES_SUMMARY_ERROR"
                )

    async def get_top_products(self, limit: int = 10) -> ServiceResult[list[dict[str, Any]]]:
        """Get top performing products"""
        async with self.track_operation("get_top_products"):
            try:
                # Simulate data retrieval
                await asyncio.sleep(0.05)

                products = [
                    {
                        "product_id": f"prod_{i}",
                        "name": f"Product {chr(65 + i)}",
                        "sales": 100000 - (i * 10000),
                        "units_sold": 1000 - (i * 100),
                        "revenue_contribution": round((100000 - (i * 10000)) / 1000000 * 100, 2),
                    }
                    for i in range(limit)
                ]

                return ServiceResult.success_result(
                    products, metadata={"total_products": len(products)}
                )

            except Exception as e:
                return ServiceResult.error_result(
                    error=f"Failed to get top products: {str(e)}", error_code="TOP_PRODUCTS_ERROR"
                )


class UserManagementService(BaseAPIService):
    """Service for user management operations"""

    def __init__(self):
        super().__init__("user_management", ServicePriority.CRITICAL)
        self._user_cache = {}

    async def _initialize(self) -> None:
        """Initialize user management service"""
        self.add_dependency("database_service")
        self.add_dependency("auth_service")
        logger.info("User Management Service initialized")

    async def _shutdown(self) -> None:
        """Shutdown user management service"""
        self._user_cache.clear()
        logger.info("User Management Service shutdown")

    async def _health_check(self) -> bool:
        """Health check for user management service"""
        return await self.check_dependencies()

    async def get_user_profile(self, user_id: str) -> ServiceResult[dict[str, Any]]:
        """Get user profile by ID"""
        async with self.track_operation("get_user_profile"):
            try:
                # Check cache first
                if user_id in self._user_cache:
                    return ServiceResult.success_result(
                        self._user_cache[user_id], metadata={"cached": True}
                    )

                # Simulate database lookup
                await asyncio.sleep(0.02)

                user_profile = {
                    "user_id": user_id,
                    "username": f"user_{user_id}",
                    "email": f"user_{user_id}@example.com",
                    "roles": ["user"],
                    "permissions": ["read", "write"],
                    "created_at": datetime.utcnow().isoformat(),
                    "last_login": datetime.utcnow().isoformat(),
                    "is_active": True,
                    "profile": {
                        "first_name": "John",
                        "last_name": "Doe",
                        "department": "Engineering",
                        "location": "Remote",
                    },
                }

                # Cache the result
                self._user_cache[user_id] = user_profile

                return ServiceResult.success_result(user_profile, metadata={"cached": False})

            except Exception as e:
                return ServiceResult.error_result(
                    error=f"Failed to get user profile: {str(e)}", error_code="USER_PROFILE_ERROR"
                )

    async def update_user_permissions(
        self, user_id: str, permissions: list[str]
    ) -> ServiceResult[bool]:
        """Update user permissions"""
        async with self.track_operation("update_user_permissions"):
            try:
                # Validate permissions
                valid_permissions = {"read", "write", "delete", "admin", "execute"}
                invalid_perms = set(permissions) - valid_permissions

                if invalid_perms:
                    return ServiceResult.error_result(
                        error=f"Invalid permissions: {invalid_perms}",
                        error_code="INVALID_PERMISSIONS",
                    )

                # Simulate database update
                await asyncio.sleep(0.05)

                # Update cache if user exists
                if user_id in self._user_cache:
                    self._user_cache[user_id]["permissions"] = permissions

                return ServiceResult.success_result(
                    True, metadata={"updated_permissions": permissions}
                )

            except Exception as e:
                return ServiceResult.error_result(
                    error=f"Failed to update user permissions: {str(e)}",
                    error_code="UPDATE_PERMISSIONS_ERROR",
                )


class NotificationService(BaseAPIService):
    """Service for sending notifications"""

    def __init__(self):
        super().__init__("notification_service", ServicePriority.NORMAL)
        self._notification_queue = []

    async def _initialize(self) -> None:
        """Initialize notification service"""
        self.add_dependency("email_service")
        logger.info("Notification Service initialized")

    async def _shutdown(self) -> None:
        """Shutdown notification service"""
        logger.info("Notification Service shutdown")

    async def _health_check(self) -> bool:
        """Health check for notification service"""
        return True

    async def send_notification(
        self, user_id: str, message: str, notification_type: str = "info", priority: str = "normal"
    ) -> ServiceResult[str]:
        """Send notification to user"""
        async with self.track_operation("send_notification"):
            try:
                notification_id = f"notif_{int(datetime.utcnow().timestamp())}_{user_id}"

                notification = {
                    "id": notification_id,
                    "user_id": user_id,
                    "message": message,
                    "type": notification_type,
                    "priority": priority,
                    "created_at": datetime.utcnow().isoformat(),
                    "status": "sent",
                }

                self._notification_queue.append(notification)

                # Simulate notification sending
                await asyncio.sleep(0.01)

                return ServiceResult.success_result(
                    notification_id,
                    metadata={"notification_type": notification_type, "priority": priority},
                )

            except Exception as e:
                return ServiceResult.error_result(
                    error=f"Failed to send notification: {str(e)}", error_code="NOTIFICATION_ERROR"
                )


class APIServiceManager:
    """
    Central manager for all API services.
    Handles service lifecycle, health monitoring, and dependency injection.
    """

    def __init__(self):
        self._services: dict[str, BaseAPIService] = {}
        self._service_order: list[str] = []  # Initialization order based on dependencies
        self._is_running = False
        self._health_check_task: asyncio.Task | None = None
        self._health_check_interval = 30  # seconds

        logger.info("API Service Manager initialized")

    def register_service(self, service: BaseAPIService) -> None:
        """Register a service with the manager"""
        self._services[service.name] = service

        # Register with service locator
        register_singleton(service.name, lambda: service)

        logger.info(f"Registered service: {service.name}")

    def get_service(self, name: str) -> BaseAPIService | None:
        """Get service by name"""
        return self._services.get(name)

    def _resolve_service_dependencies(self) -> list[str]:
        """Resolve service initialization order based on dependencies"""
        # Simple topological sort for dependency resolution
        resolved = []
        unresolved = set(self._services.keys())

        def resolve_service(service_name: str, path: list[str]):
            if service_name in resolved:
                return

            if service_name in path:
                raise ValueError(
                    f"Circular dependency detected: {' -> '.join(path + [service_name])}"
                )

            service = self._services[service_name]
            for dep in service._dependencies:
                if dep in self._services:  # Only consider registered services
                    resolve_service(dep, path + [service_name])

            if service_name in unresolved:
                resolved.append(service_name)
                unresolved.remove(service_name)

        while unresolved:
            service_name = next(iter(unresolved))
            resolve_service(service_name, [])

        return resolved

    async def start_all_services(self) -> None:
        """Start all registered services in dependency order"""
        if self._is_running:
            logger.warning("Services are already running")
            return

        try:
            # Resolve service initialization order
            self._service_order = self._resolve_service_dependencies()

            # Initialize services in order
            for service_name in self._service_order:
                service = self._services[service_name]
                logger.info(f"Starting service: {service_name}")
                await service.initialize()

            self._is_running = True

            # Start health check monitoring
            self._health_check_task = asyncio.create_task(self._health_check_loop())

            logger.info("All services started successfully")

        except Exception as e:
            logger.error(f"Failed to start services: {e}")
            await self.stop_all_services()
            raise

    async def stop_all_services(self) -> None:
        """Stop all services in reverse dependency order"""
        if not self._is_running:
            return

        # Stop health check monitoring
        if self._health_check_task:
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                pass

        # Shutdown services in reverse order
        for service_name in reversed(self._service_order):
            service = self._services[service_name]
            try:
                logger.info(f"Stopping service: {service_name}")
                await service.shutdown()
            except Exception as e:
                logger.error(f"Error stopping service {service_name}: {e}")

        self._is_running = False
        logger.info("All services stopped")

    async def _health_check_loop(self) -> None:
        """Background task for health check monitoring"""
        while self._is_running:
            try:
                await asyncio.sleep(self._health_check_interval)
                await self.health_check_all_services()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Health check loop error: {e}")

    async def health_check_all_services(self) -> dict[str, bool]:
        """Perform health check on all services"""
        results = {}

        for service_name, service in self._services.items():
            try:
                health_status = await service.health_check()
                results[service_name] = health_status

                if not health_status:
                    logger.warning(f"Service {service_name} health check failed")

            except Exception as e:
                logger.error(f"Health check error for service {service_name}: {e}")
                results[service_name] = False

        return results

    def get_all_metrics(self) -> dict[str, dict[str, Any]]:
        """Get metrics for all services"""
        metrics = {}
        for service_name, service in self._services.items():
            metrics[service_name] = service.get_metrics().to_dict()
        return metrics

    def get_service_status(self) -> dict[str, Any]:
        """Get overall service manager status"""
        total_services = len(self._services)
        running_services = sum(
            1 for s in self._services.values() if s.metrics.status == ServiceStatus.RUNNING
        )
        error_services = sum(
            1 for s in self._services.values() if s.metrics.status == ServiceStatus.ERROR
        )

        return {
            "manager_status": "running" if self._is_running else "stopped",
            "total_services": total_services,
            "running_services": running_services,
            "error_services": error_services,
            "health_percentage": (running_services / total_services * 100)
            if total_services > 0
            else 0,
            "service_names": list(self._services.keys()),
            "initialization_order": self._service_order,
        }


# Global service manager instance
_service_manager: APIServiceManager | None = None


def get_service_manager() -> APIServiceManager:
    """Get global service manager instance"""
    global _service_manager
    if _service_manager is None:
        _service_manager = APIServiceManager()

        # Register default services
        _service_manager.register_service(SalesAnalyticsService())
        _service_manager.register_service(UserManagementService())
        _service_manager.register_service(NotificationService())

    return _service_manager


# Convenience functions for common service operations
async def get_sales_summary(
    start_date: datetime, end_date: datetime, filters: dict[str, Any] = None
) -> ServiceResult[dict[str, Any]]:
    """Get sales summary through service manager"""
    manager = get_service_manager()
    sales_service = manager.get_service("sales_analytics")

    if not sales_service:
        return ServiceResult.error_result("Sales analytics service not available")

    return await sales_service.get_sales_summary(start_date, end_date, filters)


async def get_user_profile(user_id: str) -> ServiceResult[dict[str, Any]]:
    """Get user profile through service manager"""
    manager = get_service_manager()
    user_service = manager.get_service("user_management")

    if not user_service:
        return ServiceResult.error_result("User management service not available")

    return await user_service.get_user_profile(user_id)


async def send_notification(
    user_id: str, message: str, notification_type: str = "info"
) -> ServiceResult[str]:
    """Send notification through service manager"""
    manager = get_service_manager()
    notification_service = manager.get_service("notification_service")

    if not notification_service:
        return ServiceResult.error_result("Notification service not available")

    return await notification_service.send_notification(user_id, message, notification_type)
