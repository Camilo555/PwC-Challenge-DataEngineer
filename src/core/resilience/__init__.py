"""
Enterprise Resilience and Error Recovery Framework
=================================================

Comprehensive resilience framework providing self-healing capabilities,
circuit breakers, retry mechanisms, and intelligent error recovery for
enterprise-grade applications.

Features:
- Self-healing system with automated recovery
- Advanced circuit breakers with adaptive behavior
- Intelligent retry mechanisms with multiple strategies
- Error pattern recognition and recovery orchestration
- Bulkhead isolation patterns
- Real-time health monitoring
- Automated escalation and alerting
- Recovery analytics and learning

Usage:
    from core.resilience import (
        create_enterprise_resilience_system,
        resilience_decorator,
        circuit_breaker,
        retry,
        self_healing_system
    )

    # Initialize enterprise resilience
    resilience = create_enterprise_resilience_system()

    # Use decorators for automatic protection
    @resilience_decorator("database_operations")
    async def database_operation():
        # Your database operation here
        pass

    # Start self-healing monitoring
    await resilience.start_monitoring()
"""

# Core resilience components
from .self_healing_system import (
    SelfHealingSystem,
    HealthChecker,
    RecoveryAgent,
    HealthStatus,
    RecoveryAction,
    RecoveryPriority,
    HealthMetrics,
    RecoveryPlan,
    RecoveryResult,
    DatabaseHealthChecker,
    APIHealthChecker,
    DatabaseRecoveryAgent,
    APIRecoveryAgent,
    create_enterprise_self_healing_system,
    self_healing_system
)

# Circuit breaker and retry system
from .circuit_breaker_system import (
    CircuitBreaker,
    RetryMechanism,
    Bulkhead,
    ResilienceDecorator,
    ResilienceManager,
    CircuitState,
    RetryStrategy,
    BulkheadType,
    CircuitBreakerConfig,
    RetryConfig,
    BulkheadConfig,
    CircuitBreakerMetrics,
    CallResult,
    CircuitBreakerOpenException,
    BulkheadRejectedException,
    TimeoutException,
    circuit_breaker,
    retry,
    bulkhead,
    resilience_manager
)

# Error recovery orchestration
from .error_recovery_orchestrator import (
    ErrorRecoveryOrchestrator,
    ErrorAnalyzer,
    RecoveryExecutor,
    ErrorPattern,
    ErrorIncident,
    RecoveryAttempt,
    ErrorSeverity,
    RecoveryStrategy,
    EscalationLevel,
    create_error_recovery_orchestrator
)

from core.logging import get_logger

logger = get_logger(__name__)


class EnterpriseResilienceSystem:
    """
    Comprehensive enterprise resilience system that integrates all components.
    """

    def __init__(self):
        self.self_healing_system = create_enterprise_self_healing_system()
        self.resilience_manager = resilience_manager
        self.error_recovery_orchestrator = create_error_recovery_orchestrator(
            self.resilience_manager,
            self.self_healing_system
        )
        self.monitoring_enabled = False

    async def start_monitoring(self):
        """Start all monitoring and self-healing capabilities."""
        if self.monitoring_enabled:
            logger.warning("Resilience monitoring is already enabled")
            return

        try:
            # Start self-healing monitoring
            await self.self_healing_system.start_monitoring()

            self.monitoring_enabled = True
            logger.info("Enterprise resilience system monitoring started")

        except Exception as e:
            logger.error(f"Failed to start resilience monitoring: {e}")
            raise

    async def stop_monitoring(self):
        """Stop all monitoring and cleanup resources."""
        if not self.monitoring_enabled:
            return

        try:
            # Stop self-healing monitoring
            await self.self_healing_system.stop_monitoring()

            self.monitoring_enabled = False
            logger.info("Enterprise resilience system monitoring stopped")

        except Exception as e:
            logger.error(f"Failed to stop resilience monitoring: {e}")

    async def handle_error(self, error: Exception, context: dict = None) -> bool:
        """
        Handle error through the recovery orchestrator.

        Args:
            error: The exception to handle
            context: Additional context information

        Returns:
            bool: True if error was recovered, False otherwise
        """
        return await self.error_recovery_orchestrator.handle_error(error, context)

    def create_resilient_decorator(
        self,
        name: str,
        circuit_breaker_config: CircuitBreakerConfig = None,
        retry_config: RetryConfig = None,
        bulkhead_config: BulkheadConfig = None,
        timeout: float = None,
        auto_recovery: bool = True
    ) -> ResilienceDecorator:
        """
        Create a resilient decorator with automatic error recovery.

        Args:
            name: Name for the resilience pattern
            circuit_breaker_config: Circuit breaker configuration
            retry_config: Retry configuration
            bulkhead_config: Bulkhead configuration
            timeout: Operation timeout
            auto_recovery: Enable automatic error recovery

        Returns:
            ResilienceDecorator: Configured resilience decorator
        """
        decorator = self.resilience_manager.register_resilience_pattern(
            name=name,
            circuit_breaker_config=circuit_breaker_config,
            retry_config=retry_config,
            bulkhead_config=bulkhead_config,
            timeout=timeout
        )

        if auto_recovery:
            # Wrap decorator to integrate with error recovery
            original_execute = decorator.execute

            async def enhanced_execute(func, *args, **kwargs):
                try:
                    return await original_execute(func, *args, **kwargs)
                except Exception as e:
                    # Attempt recovery through orchestrator
                    recovery_context = {
                        "component_name": name,
                        "original_function": func,
                        "args": args,
                        "kwargs": kwargs
                    }

                    recovered = await self.handle_error(e, recovery_context)
                    if not recovered:
                        raise  # Re-raise if recovery failed

                    # Try one more time after recovery
                    return await original_execute(func, *args, **kwargs)

            decorator.execute = enhanced_execute

        return decorator

    def add_health_checker(self, component_type: str, checker: HealthChecker):
        """Add custom health checker."""
        self.self_healing_system.register_health_checker(component_type, checker)

    def add_recovery_agent(self, agent: RecoveryAgent):
        """Add custom recovery agent."""
        self.self_healing_system.register_recovery_agent(agent)

    def add_error_pattern(self, pattern: ErrorPattern):
        """Add custom error pattern."""
        self.error_recovery_orchestrator.add_error_pattern(pattern)

    def add_monitored_component(self, component_name: str, component_type: str):
        """Add component to monitoring."""
        self.self_healing_system.add_monitored_component(component_name, component_type)

    def get_system_health(self) -> dict:
        """Get comprehensive system health information."""
        return {
            "self_healing": self.self_healing_system.get_system_health_summary(),
            "resilience_patterns": self.resilience_manager.get_all_metrics(),
            "error_recovery": self.error_recovery_orchestrator.get_metrics(),
            "monitoring_enabled": self.monitoring_enabled
        }

    def get_incident_summary(self) -> dict:
        """Get current incident summary."""
        return {
            "active_incidents": self.error_recovery_orchestrator.get_active_incidents(),
            "recovery_history": self.error_recovery_orchestrator.get_recovery_history(limit=20),
            "self_healing_history": self.self_healing_system.get_recovery_history(limit=20)
        }


def create_enterprise_resilience_system() -> EnterpriseResilienceSystem:
    """
    Create enterprise resilience system with default configuration.

    Returns:
        EnterpriseResilienceSystem: Configured resilience system
    """
    return EnterpriseResilienceSystem()


def resilience_decorator(
    pattern_name: str,
    circuit_breaker_config: CircuitBreakerConfig = None,
    retry_config: RetryConfig = None,
    bulkhead_config: BulkheadConfig = None,
    timeout: float = None,
    auto_recovery: bool = True
):
    """
    Decorator for applying resilience patterns to functions.

    Args:
        pattern_name: Name for the resilience pattern
        circuit_breaker_config: Circuit breaker configuration
        retry_config: Retry configuration
        bulkhead_config: Bulkhead configuration
        timeout: Operation timeout
        auto_recovery: Enable automatic error recovery

    Returns:
        Decorator function
    """
    def decorator(func):
        # Get or create global resilience system
        resilience_system = _get_global_resilience_system()

        # Create resilient decorator
        resilience_dec = resilience_system.create_resilient_decorator(
            name=pattern_name,
            circuit_breaker_config=circuit_breaker_config,
            retry_config=retry_config,
            bulkhead_config=bulkhead_config,
            timeout=timeout,
            auto_recovery=auto_recovery
        )

        async def async_wrapper(*args, **kwargs):
            return await resilience_dec.execute(func, *args, **kwargs)

        def sync_wrapper(*args, **kwargs):
            import asyncio
            return asyncio.run(resilience_dec.execute(func, *args, **kwargs))

        # Return appropriate wrapper based on function type
        import asyncio
        if asyncio.iscoroutinefunction(func):
            return async_wrapper
        else:
            return sync_wrapper

    return decorator


# Global resilience system instance
_global_resilience_system: EnterpriseResilienceSystem = None


def _get_global_resilience_system() -> EnterpriseResilienceSystem:
    """Get or create global resilience system instance."""
    global _global_resilience_system
    if _global_resilience_system is None:
        _global_resilience_system = create_enterprise_resilience_system()
    return _global_resilience_system


def get_resilience_system() -> EnterpriseResilienceSystem:
    """Get the global resilience system instance."""
    return _get_global_resilience_system()


# Convenience function for common patterns
def database_resilience_decorator(
    name: str = "database_operation",
    max_failures: int = 3,
    recovery_timeout: int = 30,
    max_retries: int = 3,
    max_concurrent: int = 20
):
    """
    Decorator optimized for database operations.

    Args:
        name: Pattern name
        max_failures: Circuit breaker failure threshold
        recovery_timeout: Circuit breaker recovery timeout
        max_retries: Maximum retry attempts
        max_concurrent: Maximum concurrent operations

    Returns:
        Decorator function
    """
    circuit_config = CircuitBreakerConfig(
        failure_threshold=max_failures,
        recovery_timeout=recovery_timeout,
        timeout=10.0,
        expected_exception_types=[ConnectionError, TimeoutError]
    )

    retry_config = RetryConfig(
        max_attempts=max_retries,
        strategy=RetryStrategy.EXPONENTIAL_BACKOFF,
        initial_delay=1.0,
        max_delay=10.0
    )

    bulkhead_config = BulkheadConfig(
        max_concurrent=max_concurrent,
        max_wait_duration=5.0
    )

    return resilience_decorator(
        pattern_name=name,
        circuit_breaker_config=circuit_config,
        retry_config=retry_config,
        bulkhead_config=bulkhead_config,
        timeout=15.0
    )


def api_resilience_decorator(
    name: str = "api_operation",
    max_failures: int = 5,
    recovery_timeout: int = 60,
    max_retries: int = 4,
    max_concurrent: int = 10
):
    """
    Decorator optimized for external API calls.

    Args:
        name: Pattern name
        max_failures: Circuit breaker failure threshold
        recovery_timeout: Circuit breaker recovery timeout
        max_retries: Maximum retry attempts
        max_concurrent: Maximum concurrent operations

    Returns:
        Decorator function
    """
    circuit_config = CircuitBreakerConfig(
        failure_threshold=max_failures,
        recovery_timeout=recovery_timeout,
        timeout=30.0,
        slow_call_threshold=2.0
    )

    retry_config = RetryConfig(
        max_attempts=max_retries,
        strategy=RetryStrategy.RANDOM_JITTER,
        initial_delay=2.0,
        max_delay=30.0,
        jitter_factor=0.2
    )

    bulkhead_config = BulkheadConfig(
        max_concurrent=max_concurrent,
        max_wait_duration=10.0
    )

    return resilience_decorator(
        pattern_name=name,
        circuit_breaker_config=circuit_config,
        retry_config=retry_config,
        bulkhead_config=bulkhead_config,
        timeout=45.0
    )


# Export key components
__all__ = [
    # Main system
    "EnterpriseResilienceSystem",
    "create_enterprise_resilience_system",
    "get_resilience_system",

    # Decorators
    "resilience_decorator",
    "database_resilience_decorator",
    "api_resilience_decorator",

    # Self-healing system
    "SelfHealingSystem",
    "HealthChecker",
    "RecoveryAgent",
    "HealthStatus",
    "RecoveryAction",
    "RecoveryPriority",
    "self_healing_system",

    # Circuit breaker and retry
    "CircuitBreaker",
    "RetryMechanism",
    "Bulkhead",
    "ResilienceDecorator",
    "ResilienceManager",
    "CircuitState",
    "RetryStrategy",
    "BulkheadType",
    "CircuitBreakerConfig",
    "RetryConfig",
    "BulkheadConfig",
    "circuit_breaker",
    "retry",
    "bulkhead",
    "resilience_manager",

    # Error recovery
    "ErrorRecoveryOrchestrator",
    "ErrorPattern",
    "ErrorSeverity",
    "RecoveryStrategy",
    "EscalationLevel",

    # Exceptions
    "CircuitBreakerOpenException",
    "BulkheadRejectedException",
    "TimeoutException",
]