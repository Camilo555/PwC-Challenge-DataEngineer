"""
Dependency Injection Container
Implements the Dependency Inversion Principle with clean architecture support.
"""

import functools
import inspect
from collections.abc import Callable
from typing import Any, TypeVar

T = TypeVar('T')


class DIContainer:
    """
    Simple dependency injection container supporting:
    - Interface/implementation binding
    - Singleton and transient lifetimes
    - Constructor injection
    - Circular dependency detection
    """

    def __init__(self):
        self._bindings: dict[str, Binding] = {}
        self._instances: dict[str, Any] = {}
        self._resolution_stack: list[str] = []

    def bind(self, interface: type[T], implementation: type[T], lifetime: str = 'transient') -> 'DIContainer':
        """
        Bind interface to implementation.
        
        Args:
            interface: Interface type to bind
            implementation: Implementation type
            lifetime: 'singleton' or 'transient'
        """
        key = self._get_type_key(interface)
        self._bindings[key] = Binding(interface, implementation, lifetime)
        return self

    def bind_instance(self, interface: type[T], instance: T) -> 'DIContainer':
        """Bind interface to a specific instance (singleton)."""
        key = self._get_type_key(interface)
        self._bindings[key] = Binding(interface, type(instance), 'singleton')
        self._instances[key] = instance
        return self

    def resolve(self, interface: type[T]) -> T:
        """Resolve an interface to its implementation."""
        key = self._get_type_key(interface)

        # Check for circular dependencies
        if key in self._resolution_stack:
            cycle = ' -> '.join(self._resolution_stack + [key])
            raise ValueError(f"Circular dependency detected: {cycle}")

        try:
            self._resolution_stack.append(key)
            return self._resolve_internal(interface, key)
        finally:
            self._resolution_stack.remove(key)

    def _resolve_internal(self, interface: type[T], key: str) -> T:
        """Internal resolution logic."""
        # Check if we have a binding
        if key not in self._bindings:
            # Try to create directly if it's a concrete class
            if not inspect.isabstract(interface):
                return self._create_instance(interface)
            raise ValueError(f"No binding found for {interface.__name__}")

        binding = self._bindings[key]

        # Return existing singleton instance
        if binding.lifetime == 'singleton' and key in self._instances:
            return self._instances[key]

        # Create new instance
        instance = self._create_instance(binding.implementation)

        # Store singleton instance
        if binding.lifetime == 'singleton':
            self._instances[key] = instance

        return instance

    def _create_instance(self, cls: type[T]) -> T:
        """Create instance with constructor injection."""
        # Get constructor parameters
        signature = inspect.signature(cls.__init__)
        parameters = {}

        for param_name, param in signature.parameters.items():
            if param_name == 'self':
                continue

            # Skip parameters with default values for now
            if param.default is not inspect.Parameter.empty:
                continue

            # Resolve parameter type
            if param.annotation is not inspect.Parameter.empty:
                param_value = self.resolve(param.annotation)
                parameters[param_name] = param_value

        return cls(**parameters)

    def _get_type_key(self, type_: type) -> str:
        """Get string key for type."""
        return f"{type_.__module__}.{type_.__name__}"


class Binding:
    """Represents a binding between interface and implementation."""

    def __init__(self, interface: type, implementation: type, lifetime: str):
        self.interface = interface
        self.implementation = implementation
        self.lifetime = lifetime


# Global container instance
_container = DIContainer()


def get_container() -> DIContainer:
    """Get the global DI container."""
    return _container


def inject(interface: type[T]) -> Callable[[], T]:
    """
    Decorator for dependency injection.
    
    Usage:
        @inject(ISalesRepository)
        def get_sales_repository() -> ISalesRepository:
            pass  # Implementation is replaced by decorator
    """
    def decorator(func: Callable[[], T]) -> Callable[[], T]:
        @functools.wraps(func)
        def wrapper() -> T:
            return _container.resolve(interface)
        return wrapper
    return decorator


class ServiceLifetime:
    """Service lifetime constants."""
    SINGLETON = 'singleton'
    TRANSIENT = 'transient'


def configure_dependencies() -> None:
    """Configure all application dependencies."""
    from data_access.patterns.unit_of_work import IUnitOfWork, UnitOfWork
    from data_access.repositories.sales_repository import SalesRepository
    from domain.interfaces.sales_repository import ISalesRepository
    from domain.interfaces.sales_service import ISalesService
    from domain.mappers.model_mapper import ModelMapper
    from domain.services.sales_service import SalesService

    container = get_container()

    # Repository bindings
    container.bind(ISalesRepository, SalesRepository, ServiceLifetime.TRANSIENT)
    container.bind(IUnitOfWork, UnitOfWork, ServiceLifetime.TRANSIENT)

    # Service bindings
    container.bind(ISalesService, SalesService, ServiceLifetime.TRANSIENT)

    # Infrastructure bindings
    container.bind(ModelMapper, ModelMapper, ServiceLifetime.SINGLETON)


# Convenience functions for common patterns
def get_service(interface: type[T]) -> T:
    """Get service instance from container."""
    return _container.resolve(interface)


def reset_container() -> None:
    """Reset container (mainly for testing)."""
    global _container
    _container = DIContainer()
