"""
Tracing Startup Integration
Automatic configuration and initialization of distributed tracing on application startup.
"""

import os
from typing import Dict, Any, Optional

from .otel_config import configure_opentelemetry, TracingConfig, is_tracing_enabled
from .instrumentation import auto_instrument_all, get_instrumentation_status
from core.logging import get_logger

logger = get_logger(__name__)


class TracingStartupManager:
    """Manages tracing initialization during application startup."""
    
    def __init__(self):
        self.config: Optional[TracingConfig] = None
        self.initialized = False
        self.instrumentation_results = {}
    
    async def initialize_tracing(
        self, 
        config: Optional[TracingConfig] = None,
        auto_instrument: bool = True
    ) -> Dict[str, Any]:
        """
        Initialize distributed tracing system.
        
        Args:
            config: Tracing configuration (uses environment if None)
            auto_instrument: Whether to auto-instrument libraries
            
        Returns:
            Dictionary with initialization results
        """
        results = {
            'status': 'success',
            'tracing_enabled': False,
            'configuration': {},
            'instrumentation': {},
            'errors': [],
            'warnings': []
        }
        
        try:
            # Configure OpenTelemetry
            self.config = config or TracingConfig.from_environment()
            results['configuration'] = self._config_to_dict(self.config)
            
            if self.config.enabled:
                success = configure_opentelemetry(self.config)
                
                if success:
                    results['tracing_enabled'] = True
                    logger.info(f"OpenTelemetry configured with {self.config.backend} backend")
                    
                    # Auto-instrument libraries if enabled
                    if auto_instrument and self.config.auto_instrument:
                        self.instrumentation_results = auto_instrument_all()
                        results['instrumentation'] = self.instrumentation_results
                        
                        # Log instrumentation summary
                        successful = sum(1 for r in self.instrumentation_results.values() if r)
                        total = len(self.instrumentation_results)
                        logger.info(f"Auto-instrumentation: {successful}/{total} libraries")
                    
                    self.initialized = True
                    
                else:
                    results['status'] = 'error'
                    results['errors'].append("Failed to configure OpenTelemetry")
                    
            else:
                results['tracing_enabled'] = False
                results['warnings'].append("Tracing disabled by configuration")
                logger.info("Distributed tracing disabled")
            
        except Exception as e:
            results['status'] = 'error'
            results['errors'].append(f"Tracing initialization failed: {str(e)}")
            logger.error(f"Failed to initialize tracing: {e}")
        
        return results
    
    def _config_to_dict(self, config: TracingConfig) -> Dict[str, Any]:
        """Convert TracingConfig to dictionary for logging."""
        return {
            'enabled': config.enabled,
            'service_name': config.service_name,
            'service_version': config.service_version,
            'environment': config.environment,
            'backend': config.backend.value,
            'endpoint': config.endpoint,
            'sampling_rate': config.sampling_rate,
            'auto_instrument': config.auto_instrument
        }
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get tracing system health status."""
        status = {
            'initialized': self.initialized,
            'tracing_enabled': is_tracing_enabled(),
            'configuration': {},
            'instrumentation_status': {}
        }
        
        if self.config:
            status['configuration'] = self._config_to_dict(self.config)
        
        if self.instrumentation_results:
            status['instrumentation_status'] = self.instrumentation_results
        
        # Get current instrumentation status
        try:
            current_status = get_instrumentation_status()
            status['current_instrumentation'] = current_status
        except Exception as e:
            logger.debug(f"Failed to get instrumentation status: {e}")
        
        return status
    
    def shutdown(self) -> None:
        """Shutdown tracing system."""
        if self.initialized:
            try:
                from .otel_config import shutdown_tracing
                shutdown_tracing()
                self.initialized = False
                logger.info("Tracing system shut down")
            except Exception as e:
                logger.error(f"Failed to shutdown tracing: {e}")


# Global startup manager
_startup_manager: Optional[TracingStartupManager] = None


def get_tracing_startup_manager() -> TracingStartupManager:
    """Get or create global tracing startup manager."""
    global _startup_manager
    
    if _startup_manager is None:
        _startup_manager = TracingStartupManager()
    
    return _startup_manager


async def initialize_tracing(config: Optional[TracingConfig] = None) -> Dict[str, Any]:
    """
    Convenience function to initialize tracing.
    
    Args:
        config: Tracing configuration
        
    Returns:
        Initialization results
    """
    manager = get_tracing_startup_manager()
    return await manager.initialize_tracing(config)


def get_tracing_health() -> Dict[str, Any]:
    """Get tracing system health status."""
    manager = get_tracing_startup_manager()
    return manager.get_health_status()


def shutdown_tracing_system() -> None:
    """Shutdown tracing system."""
    manager = get_tracing_startup_manager()
    manager.shutdown()


# Integration with main startup system
async def setup_tracing_for_startup() -> Dict[str, Any]:
    """
    Set up tracing during application startup.
    Called by the main startup system.
    """
    logger.info("🔍 Initializing distributed tracing...")
    
    try:
        # Check if tracing should be enabled
        environment = os.getenv('ENVIRONMENT', 'development').lower()
        tracing_enabled = os.getenv('OTEL_ENABLED', 'true').lower() in ('true', '1', 'yes')
        
        if not tracing_enabled:
            logger.info("Distributed tracing disabled by configuration")
            return {
                'status': 'disabled',
                'tracing_enabled': False,
                'message': 'Disabled by configuration'
            }
        
        # Initialize tracing
        results = await initialize_tracing()
        
        if results['status'] == 'success' and results['tracing_enabled']:
            logger.info("✅ Distributed tracing initialized successfully")
            
            # Log configuration summary
            config = results.get('configuration', {})
            logger.info(f"Tracing backend: {config.get('backend', 'unknown')}")
            logger.info(f"Service: {config.get('service_name', 'unknown')}")
            
            # Log instrumentation summary
            instrumentation = results.get('instrumentation', {})
            if instrumentation:
                successful = sum(1 for r in instrumentation.values() if r)
                total = len(instrumentation)
                logger.info(f"Instrumented libraries: {successful}/{total}")
        
        elif results['status'] == 'error':
            logger.error("❌ Failed to initialize distributed tracing")
            for error in results.get('errors', []):
                logger.error(f"  - {error}")
        
        return results
        
    except Exception as e:
        logger.error(f"Tracing startup failed: {e}")
        return {
            'status': 'error',
            'tracing_enabled': False,
            'errors': [str(e)]
        }


# Environment-specific configuration helpers
def get_development_config() -> TracingConfig:
    """Get tracing configuration for development environment."""
    return TracingConfig(
        enabled=True,
        service_name="retail-etl-dev",
        environment="development", 
        backend="console",
        sampling_rate=1.0,
        auto_instrument=True
    )


def get_production_config() -> TracingConfig:
    """Get tracing configuration for production environment."""
    return TracingConfig(
        enabled=True,
        service_name="retail-etl-prod",
        environment="production",
        backend="jaeger",  # or "otlp"
        endpoint=os.getenv('JAEGER_ENDPOINT', 'http://jaeger:14268/api/traces'),
        sampling_rate=0.1,  # 10% sampling in production
        auto_instrument=True,
        resource_attributes={
            'deployment.environment': 'production',
            'service.version': os.getenv('SERVICE_VERSION', '1.0.0')
        }
    )


def get_config_for_environment(environment: str) -> TracingConfig:
    """
    Get tracing configuration for specific environment.
    
    Args:
        environment: Environment name (development, staging, production)
        
    Returns:
        TracingConfig appropriate for the environment
    """
    environment = environment.lower()
    
    if environment == 'production':
        return get_production_config()
    elif environment == 'staging':
        config = get_production_config()
        config.service_name = "retail-etl-staging"
        config.environment = "staging"
        config.sampling_rate = 0.5  # 50% sampling in staging
        return config
    else:
        return get_development_config()