"""
Service Locator Pattern for API Dependencies
Provides centralized dependency management and resolves circular import issues.
"""

import logging
from threading import Lock
from typing import Any, TypeVar

logger = logging.getLogger(__name__)

T = TypeVar("T")


class ServiceLocator:
    """
    Centralized service locator for managing API dependencies.
    Provides lazy loading and circular import resolution.
    """

    def __init__(self):
        self._services: dict[str, Any] = {}
        self._factories: dict[str, callable] = {}
        self._singletons: dict[str, Any] = {}
        self._lock = Lock()

    def register_service(self, name: str, service: Any) -> None:
        """Register a service instance"""
        with self._lock:
            self._services[name] = service
            logger.debug(f"Registered service: {name}")

    def register_factory(self, name: str, factory: callable) -> None:
        """Register a factory function for lazy loading"""
        with self._lock:
            self._factories[name] = factory
            logger.debug(f"Registered factory: {name}")

    def register_singleton(self, name: str, factory: callable) -> None:
        """Register a singleton factory function"""
        with self._lock:
            self._factories[name] = factory
            self._singletons[name] = None  # Placeholder
            logger.debug(f"Registered singleton factory: {name}")

    def get_service(self, name: str, default: Any = None) -> Any:
        """Get a service by name"""
        # Check direct services first
        if name in self._services:
            return self._services[name]

        # Check singletons
        if name in self._singletons:
            if self._singletons[name] is None:
                with self._lock:
                    if self._singletons[name] is None:  # Double-check locking
                        if name in self._factories:
                            try:
                                self._singletons[name] = self._factories[name]()
                                logger.debug(f"Created singleton: {name}")
                            except Exception as e:
                                logger.error(f"Failed to create singleton {name}: {e}")
                                return default
            return self._singletons[name]

        # Check factories
        if name in self._factories:
            try:
                service = self._factories[name]()
                logger.debug(f"Created service: {name}")
                return service
            except Exception as e:
                logger.error(f"Failed to create service {name}: {e}")
                return default

        logger.warning(f"Service not found: {name}")
        return default

    def is_available(self, name: str) -> bool:
        """Check if a service is available"""
        return name in self._services or name in self._factories or name in self._singletons

    def clear(self) -> None:
        """Clear all registered services (useful for testing)"""
        with self._lock:
            self._services.clear()
            self._factories.clear()
            self._singletons.clear()
            logger.debug("Cleared all services")


# Global service locator instance
_service_locator = ServiceLocator()


def get_service_locator() -> ServiceLocator:
    """Get the global service locator instance"""
    return _service_locator


def register_service(name: str, service: Any) -> None:
    """Register a service in the global locator"""
    _service_locator.register_service(name, service)


def register_factory(name: str, factory: callable) -> None:
    """Register a factory in the global locator"""
    _service_locator.register_factory(name, factory)


def register_singleton(name: str, factory: callable) -> None:
    """Register a singleton in the global locator"""
    _service_locator.register_singleton(name, factory)


def get_service(name: str, default: Any = None) -> Any:
    """Get a service from the global locator"""
    return _service_locator.get_service(name, default)


def is_service_available(name: str) -> bool:
    """Check if a service is available in the global locator"""
    return _service_locator.is_available(name)


# Service registration helpers for common API services
def register_microservices_components():
    """Register microservices components with lazy loading"""

    def create_service_registry():
        try:
            from api.gateway.service_registry import ServiceRegistry

            return ServiceRegistry()
        except ImportError as e:
            logger.warning(f"ServiceRegistry not available: {e}")
            return None

    def create_api_gateway():
        try:
            from api.gateway.api_gateway import APIGateway

            return APIGateway()
        except ImportError as e:
            logger.warning(f"APIGateway not available: {e}")
            return None

    def create_saga_orchestrator():
        try:
            from api.patterns.saga_orchestrator import SagaOrchestrator

            return SagaOrchestrator()
        except ImportError as e:
            logger.warning(f"SagaOrchestrator not available: {e}")
            return None

    def create_cqrs_framework():
        try:
            from api.patterns.cqrs_framework import CQRSFramework

            return CQRSFramework()
        except ImportError as e:
            logger.warning(f"CQRSFramework not available: {e}")
            return None

    # Register all components as singletons
    register_singleton("service_registry", create_service_registry)
    register_singleton("api_gateway", create_api_gateway)
    register_singleton("saga_orchestrator", create_saga_orchestrator)
    register_singleton("cqrs_framework", create_cqrs_framework)

    logger.info("Registered microservices components")


# Auto-register components on module import
register_microservices_components()
