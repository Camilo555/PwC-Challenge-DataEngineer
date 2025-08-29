"""
Comprehensive DataDog APM Initialization and Startup Module
Provides centralized initialization, configuration, and management for DataDog APM across the enterprise data platform
"""

import asyncio
import os
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Tuple

# DataDog imports
from ddtrace import patch_all, patch, tracer, config
from ddtrace.contrib.asyncio import context_provider
from ddtrace.filters import FilterRequestsOnUrl
from ddtrace.profiling import Profiler
from ddtrace.runtime import RuntimeMetrics

# Core imports
from core.config import get_settings
from core.logging import get_logger
from monitoring.datadog_apm_config import (
    get_datadog_apm_config_manager, 
    DataDogAPMConfiguration,
    Environment
)
from monitoring.datadog_integration import create_datadog_monitoring, DatadogMonitoring

logger = get_logger(__name__)
settings = get_settings()


class DataDogAPMInitializer:
    """
    Comprehensive DataDog APM initializer for enterprise data platform
    
    Features:
    - Automatic instrumentation with selective patching
    - Performance profiling setup
    - Runtime metrics collection
    - Service discovery integration
    - Health monitoring and diagnostics
    - Environment-specific configuration
    """
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.config_manager = get_datadog_apm_config_manager()
        self.configuration = self.config_manager.get_active_configuration()
        self.profiler: Optional[Profiler] = None
        self.runtime_metrics: Optional[RuntimeMetrics] = None
        self.datadog_monitoring: Optional[DatadogMonitoring] = None
        self.initialization_start_time = time.time()
        self.is_initialized = False
        self.patched_integrations: Set[str] = set()
        
        # Instrumentation mapping
        self.available_integrations = {
            # Web frameworks
            "fastapi": {"module": "fastapi", "priority": 1},
            "starlette": {"module": "starlette", "priority": 1},
            
            # Databases
            "sqlalchemy": {"module": "sqlalchemy", "priority": 2},
            "psycopg2": {"module": "psycopg2", "priority": 2},
            "sqlite3": {"module": "sqlite3", "priority": 2},
            
            # HTTP clients
            "requests": {"module": "requests", "priority": 3},
            "httpx": {"module": "httpx", "priority": 3},
            "aiohttp": {"module": "aiohttp", "priority": 3},
            
            # Messaging
            "pika": {"module": "pika", "priority": 4},
            "kafka": {"module": "kafka", "priority": 4},
            
            # Task queues and async
            "asyncio": {"module": "asyncio", "priority": 5},
            "celery": {"module": "celery", "priority": 5},
            
            # ML and Data Science
            "sklearn": {"module": "sklearn", "priority": 6},
            "pandas": {"module": "pandas", "priority": 6},
            
            # Caching and Search
            "elasticsearch": {"module": "elasticsearch", "priority": 7},
            "redis": {"module": "redis", "priority": 7},
            
            # Additional integrations
            "logging": {"module": "logging", "priority": 8},
            "subprocess": {"module": "subprocess", "priority": 8}
        }
    
    async def initialize_datadog_apm(self, service_name: Optional[str] = None) -> bool:
        """
        Initialize DataDog APM with comprehensive configuration
        
        Args:
            service_name: Override service name for this instance
            
        Returns:
            True if initialization successful, False otherwise
        """
        try:
            self.logger.info("Starting DataDog APM initialization...")
            
            # Set service name if provided
            if service_name:
                self.configuration.service.service_name = service_name
            
            # Step 1: Configure basic tracer settings
            self._configure_basic_tracer()
            
            # Step 2: Setup instrumentation
            await self._setup_instrumentation()
            
            # Step 3: Configure filtering
            self._configure_filters()
            
            # Step 4: Setup profiling if enabled
            if self.configuration.profiling.enabled:
                await self._setup_profiling()
            
            # Step 5: Setup runtime metrics
            if self.configuration.service.runtime_metrics_enabled:
                await self._setup_runtime_metrics()
            
            # Step 6: Initialize custom monitoring
            await self._setup_custom_monitoring()
            
            # Step 7: Configure security settings
            self._configure_security_settings()
            
            # Step 8: Setup health monitoring
            await self._setup_health_monitoring()
            
            # Step 9: Validate configuration
            await self._validate_initialization()
            
            self.is_initialized = True
            initialization_time = time.time() - self.initialization_start_time
            
            self.logger.info(
                f"DataDog APM initialization completed successfully in {initialization_time:.2f}s",
                extra={
                    "service_name": self.configuration.service.service_name,
                    "environment": self.configuration.environment.value,
                    "profiling_enabled": self.configuration.profiling.enabled,
                    "runtime_metrics_enabled": self.configuration.service.runtime_metrics_enabled,
                    "initialization_time_seconds": initialization_time,
                    "patched_integrations": list(self.patched_integrations)
                }
            )
            
            # Send initialization success metric
            if self.datadog_monitoring:
                await self.datadog_monitoring.counter(
                    "datadog.apm.initialization.success",
                    tags=[
                        f"service:{self.configuration.service.service_name}",
                        f"environment:{self.configuration.environment.value}"
                    ]
                )
                await self.datadog_monitoring.histogram(
                    "datadog.apm.initialization.duration",
                    initialization_time * 1000,  # Convert to milliseconds
                    tags=[
                        f"service:{self.configuration.service.service_name}",
                        f"environment:{self.configuration.environment.value}"
                    ]
                )
            
            return True
            
        except Exception as e:
            initialization_time = time.time() - self.initialization_start_time
            self.logger.error(
                f"DataDog APM initialization failed: {str(e)}",
                extra={
                    "error_type": type(e).__name__,
                    "initialization_time_seconds": initialization_time
                }
            )
            
            # Send initialization failure metric
            if self.datadog_monitoring:
                try:
                    await self.datadog_monitoring.counter(
                        "datadog.apm.initialization.failure",
                        tags=[
                            f"service:{self.configuration.service.service_name}",
                            f"environment:{self.configuration.environment.value}",
                            f"error_type:{type(e).__name__}"
                        ]
                    )
                except:
                    pass  # Don't fail on metric sending failure
            
            return False
    
    def _configure_basic_tracer(self):
        """Configure basic DataDog tracer settings."""
        
        # Core service configuration
        config.service = self.configuration.service.service_name
        config.env = self.configuration.environment.value
        config.version = self.configuration.service.version
        
        # Agent configuration
        config.trace.hostname = self.configuration.agent.hostname
        config.trace.port = self.configuration.agent.port
        config.trace.https = self.configuration.agent.https
        
        # Sampling configuration
        if self.configuration.service.trace_enabled:
            config.trace.enabled = True
            config.trace.sample_rate = self.configuration.service.trace_sample_rate
        else:
            config.trace.enabled = False
        
        # Analytics configuration
        if self.configuration.service.trace_analytics_enabled:
            config.analytics_enabled = True
        
        # Logs injection
        if self.configuration.service.logs_injection:
            config.logs_injection = True
        
        # Performance settings
        config.trace.partial_flush_enabled = self.configuration.performance.partial_flush_enabled
        config.trace.partial_flush_min_spans = self.configuration.performance.partial_flush_min_spans
        
        # Set global tags
        for key, value in self.configuration.service.global_tags.items():
            config.tags[key] = value
        
        # Additional environment-specific tags
        config.tags.update({
            "platform": "enterprise_data",
            "component": "apm",
            "initialization_time": datetime.utcnow().isoformat(),
            "python_version": os.sys.version.split()[0],
            "hostname": os.getenv("HOSTNAME", "unknown")
        })
        
        self.logger.debug("Basic tracer configuration completed")
    
    async def _setup_instrumentation(self):
        """Setup selective instrumentation based on configuration."""
        
        if not self.configuration.service.auto_instrument:
            self.logger.info("Automatic instrumentation disabled")
            return
        
        # Get desired integrations
        desired_integrations = set(self.configuration.service.instrument_libraries)
        
        if not desired_integrations:
            # Default to all available integrations if none specified
            desired_integrations = set(self.available_integrations.keys())
        
        # Sort by priority for proper initialization order
        sorted_integrations = sorted(
            desired_integrations,
            key=lambda x: self.available_integrations.get(x, {}).get("priority", 999)
        )
        
        self.logger.info(f"Setting up instrumentation for: {sorted_integrations}")
        
        # Patch integrations individually for better error handling
        for integration in sorted_integrations:
            if integration in self.available_integrations:
                try:
                    await self._patch_integration(integration)
                except Exception as e:
                    self.logger.warning(
                        f"Failed to patch {integration}: {str(e)}",
                        extra={"integration": integration, "error_type": type(e).__name__}
                    )
        
        # Also try automatic patching as fallback
        try:
            patch_all()
            self.logger.debug("Automatic patch_all() completed successfully")
        except Exception as e:
            self.logger.warning(f"patch_all() failed: {str(e)}")
        
        self.logger.info(f"Instrumentation setup completed. Patched: {list(self.patched_integrations)}")
    
    async def _patch_integration(self, integration_name: str):
        """Patch a specific integration with error handling."""
        
        try:
            module_name = self.available_integrations[integration_name]["module"]
            
            # Special handling for specific integrations
            if integration_name == "asyncio":
                # Set up asyncio context provider
                tracer.configure(context_provider=context_provider)
                patch(asyncio=True)
            elif integration_name == "fastapi":
                patch(fastapi=True)
            elif integration_name == "sqlalchemy":
                patch(sqlalchemy=True)
            elif integration_name == "requests":
                patch(requests=True)
            elif integration_name == "httpx":
                patch(httpx=True)
            elif integration_name == "aiohttp":
                patch(aiohttp=True)
            elif integration_name == "psycopg2":
                patch(psycopg2=True)
            elif integration_name == "pika":
                patch(pika=True)
            elif integration_name == "kafka":
                patch(kafka=True)
            elif integration_name == "elasticsearch":
                patch(elasticsearch=True)
            elif integration_name == "pandas":
                patch(pandas=True)
            elif integration_name == "logging":
                patch(logging=True)
            else:
                # Generic patching
                patch(**{integration_name: True})
            
            self.patched_integrations.add(integration_name)
            self.logger.debug(f"Successfully patched {integration_name}")
            
        except ImportError:
            self.logger.debug(f"Module {integration_name} not available for patching")
        except Exception as e:
            self.logger.warning(f"Failed to patch {integration_name}: {str(e)}")
            raise
    
    def _configure_filters(self):
        """Configure request and span filters."""
        
        filters = []
        
        # Add ignored resources
        for resource_pattern in self.configuration.service.ignored_resources:
            try:
                filters.append(FilterRequestsOnUrl(resource_pattern))
            except Exception as e:
                self.logger.warning(f"Invalid filter pattern '{resource_pattern}': {str(e)}")
        
        # Additional default filters for common health check endpoints
        default_filters = [
            r"GET /health.*",
            r"GET /ready.*", 
            r"GET /alive.*",
            r"GET /status.*",
            r"GET /metrics.*",
            r"GET /favicon\.ico.*",
            r".*\.js$",
            r".*\.css$",
            r".*\.png$",
            r".*\.jpg$",
            r".*\.gif$",
            r".*\.svg$"
        ]
        
        for filter_pattern in default_filters:
            try:
                filters.append(FilterRequestsOnUrl(filter_pattern))
            except:
                pass
        
        if filters:
            tracer.configure(settings={"FILTERS": filters})
            self.logger.debug(f"Configured {len(filters)} request filters")
    
    async def _setup_profiling(self):
        """Setup DataDog profiling if enabled."""
        
        try:
            from ddtrace.profiling import Profiler
            
            # Configure profiling
            config.profiling.enabled = True
            config.profiling.upload_period = self.configuration.profiling.upload_period
            config.profiling.max_frames = self.configuration.profiling.max_frames
            
            # Create and start profiler
            self.profiler = Profiler(
                url="https://intake.profile.datadoghq.com/v1/input",
                service=self.configuration.service.service_name,
                env=self.configuration.environment.value,
                version=self.configuration.service.version,
                tags={
                    **self.configuration.service.global_tags,
                    "component": "profiler"
                }
            )
            
            # Configure profiling types
            if not self.configuration.profiling.cpu_enabled:
                self.profiler._profilers = [p for p in self.profiler._profilers if "cpu" not in str(type(p)).lower()]
            
            if not self.configuration.profiling.memory_enabled:
                self.profiler._profilers = [p for p in self.profiler._profilers if "memory" not in str(type(p)).lower()]
            
            self.profiler.start()
            
            self.logger.info(
                "DataDog profiling started successfully",
                extra={
                    "upload_period": self.configuration.profiling.upload_period,
                    "max_frames": self.configuration.profiling.max_frames,
                    "cpu_enabled": self.configuration.profiling.cpu_enabled,
                    "memory_enabled": self.configuration.profiling.memory_enabled
                }
            )
            
        except ImportError:
            self.logger.warning("DataDog profiling not available - ddtrace profiling module not found")
        except Exception as e:
            self.logger.error(f"Failed to setup profiling: {str(e)}")
            raise
    
    async def _setup_runtime_metrics(self):
        """Setup runtime metrics collection."""
        
        try:
            config.runtime_metrics.enabled = True
            
            # Start runtime metrics collection
            self.runtime_metrics = RuntimeMetrics()
            self.runtime_metrics.enable()
            
            self.logger.info("DataDog runtime metrics collection enabled")
            
        except Exception as e:
            self.logger.error(f"Failed to setup runtime metrics: {str(e)}")
            raise
    
    async def _setup_custom_monitoring(self):
        """Setup custom DataDog monitoring integration."""
        
        try:
            # Initialize DataDog monitoring client
            self.datadog_monitoring = create_datadog_monitoring(
                service_name=self.configuration.service.service_name,
                environment=self.configuration.environment.value,
                tags=self.configuration.service.global_tags
            )
            
            self.logger.info("Custom DataDog monitoring initialized")
            
        except Exception as e:
            self.logger.warning(f"Failed to setup custom monitoring: {str(e)}")
            # Don't raise here - custom monitoring is optional
    
    def _configure_security_settings(self):
        """Configure security and compliance settings."""
        
        # SQL obfuscation
        if self.configuration.security.obfuscate_sql_values:
            config.database_query_obfuscation = True
        
        # HTTP header obfuscation
        if self.configuration.security.obfuscate_http_headers:
            config.http_header_tags = {
                header: False for header in self.configuration.security.obfuscate_http_headers
            }
        
        # Additional security configurations based on compliance requirements
        if self.configuration.security.enable_pci_compliance:
            config.trace.obfuscate_credit_card = True
        
        if self.configuration.security.enable_hipaa_compliance:
            config.trace.obfuscate_health_data = True
        
        if self.configuration.security.enable_gdpr_compliance:
            config.trace.obfuscate_personal_data = True
        
        # Configure sensitive data scrubbing
        if self.configuration.security.scrub_sensitive_data:
            sensitive_patterns = "|".join(self.configuration.security.sensitive_keys)
            config.trace.sensitive_data_patterns = sensitive_patterns
        
        self.logger.debug("Security settings configured")
    
    async def _setup_health_monitoring(self):
        """Setup health monitoring and diagnostics."""
        
        if not self.configuration.enable_health_checks:
            return
        
        # Start background health check task
        asyncio.create_task(self._health_check_loop())
        self.logger.debug("Health monitoring task started")
    
    async def _health_check_loop(self):
        """Background task for health monitoring."""
        
        while self.is_initialized:
            try:
                await self._perform_health_check()
                await asyncio.sleep(self.configuration.agent.health_check_interval)
            except Exception as e:
                self.logger.warning(f"Health check failed: {str(e)}")
                await asyncio.sleep(60)  # Wait before retrying
    
    async def _perform_health_check(self):
        """Perform DataDog agent health check."""
        
        try:
            # Check if tracer is healthy
            if tracer and hasattr(tracer, 'writer') and tracer.writer:
                # Send a test metric to verify connectivity
                if self.datadog_monitoring:
                    await self.datadog_monitoring.counter(
                        "datadog.apm.health.check",
                        tags=[f"service:{self.configuration.service.service_name}"]
                    )
                    
                    # Check profiler health
                    if self.profiler and hasattr(self.profiler, '_scheduler'):
                        await self.datadog_monitoring.counter(
                            "datadog.apm.profiler.health.check",
                            tags=[f"service:{self.configuration.service.service_name}"]
                        )
                    
                    # Check runtime metrics health
                    if self.runtime_metrics and hasattr(self.runtime_metrics, '_enabled'):
                        await self.datadog_monitoring.counter(
                            "datadog.apm.runtime_metrics.health.check",
                            tags=[f"service:{self.configuration.service.service_name}"]
                        )
                
                self.logger.debug("DataDog APM health check passed")
                
        except Exception as e:
            self.logger.warning(f"DataDog APM health check failed: {str(e)}")
            
            if self.datadog_monitoring:
                try:
                    await self.datadog_monitoring.counter(
                        "datadog.apm.health.check.failure",
                        tags=[
                            f"service:{self.configuration.service.service_name}",
                            f"error_type:{type(e).__name__}"
                        ]
                    )
                except:
                    pass
    
    async def _validate_initialization(self):
        """Validate that initialization was successful."""
        
        validation_errors = []
        
        # Check tracer configuration
        if not tracer:
            validation_errors.append("DataDog tracer not initialized")
        
        # Check service name
        if not config.service:
            validation_errors.append("Service name not configured")
        
        # Check agent connectivity (attempt to send a test span)
        try:
            with tracer.trace("datadog.apm.validation.test", service=config.service):
                pass
        except Exception as e:
            validation_errors.append(f"Failed to create test span: {str(e)}")
        
        # Check profiling if enabled
        if self.configuration.profiling.enabled and not self.profiler:
            validation_errors.append("Profiling enabled but profiler not started")
        
        # Check runtime metrics if enabled
        if self.configuration.service.runtime_metrics_enabled and not self.runtime_metrics:
            validation_errors.append("Runtime metrics enabled but not initialized")
        
        if validation_errors:
            error_msg = f"DataDog APM validation failed: {'; '.join(validation_errors)}"
            self.logger.error(error_msg)
            raise Exception(error_msg)
        
        self.logger.info("DataDog APM validation completed successfully")
    
    async def shutdown_datadog_apm(self):
        """Graceful shutdown of DataDog APM."""
        
        try:
            self.logger.info("Shutting down DataDog APM...")
            
            shutdown_start_time = time.time()
            
            # Stop profiler
            if self.profiler:
                try:
                    self.profiler.stop()
                    self.logger.debug("Profiler stopped")
                except Exception as e:
                    self.logger.warning(f"Failed to stop profiler: {str(e)}")
            
            # Disable runtime metrics
            if self.runtime_metrics:
                try:
                    self.runtime_metrics.disable()
                    self.logger.debug("Runtime metrics disabled")
                except Exception as e:
                    self.logger.warning(f"Failed to disable runtime metrics: {str(e)}")
            
            # Flush any remaining traces
            if tracer and hasattr(tracer, 'shutdown'):
                try:
                    tracer.shutdown()
                    self.logger.debug("Tracer shutdown completed")
                except Exception as e:
                    self.logger.warning(f"Failed to shutdown tracer: {str(e)}")
            
            # Send shutdown metric
            if self.datadog_monitoring:
                try:
                    shutdown_time = time.time() - shutdown_start_time
                    await self.datadog_monitoring.counter(
                        "datadog.apm.shutdown.success",
                        tags=[f"service:{self.configuration.service.service_name}"]
                    )
                    await self.datadog_monitoring.histogram(
                        "datadog.apm.shutdown.duration",
                        shutdown_time * 1000,
                        tags=[f"service:{self.configuration.service.service_name}"]
                    )
                except:
                    pass
            
            self.is_initialized = False
            
            shutdown_time = time.time() - shutdown_start_time
            self.logger.info(f"DataDog APM shutdown completed in {shutdown_time:.2f}s")
            
        except Exception as e:
            self.logger.error(f"Error during DataDog APM shutdown: {str(e)}")
    
    def get_initialization_status(self) -> Dict[str, Any]:
        """Get detailed initialization status."""
        
        return {
            "is_initialized": self.is_initialized,
            "service_name": self.configuration.service.service_name,
            "environment": self.configuration.environment.value,
            "version": self.configuration.service.version,
            "initialization_time": time.time() - self.initialization_start_time,
            "profiling_enabled": self.configuration.profiling.enabled,
            "profiler_active": self.profiler is not None,
            "runtime_metrics_enabled": self.configuration.service.runtime_metrics_enabled,
            "runtime_metrics_active": self.runtime_metrics is not None,
            "custom_monitoring_active": self.datadog_monitoring is not None,
            "patched_integrations": list(self.patched_integrations),
            "agent_configuration": {
                "hostname": self.configuration.agent.hostname,
                "port": self.configuration.agent.port,
                "https": self.configuration.agent.https
            },
            "trace_configuration": {
                "enabled": config.trace.enabled if hasattr(config, 'trace') else False,
                "sample_rate": config.trace.sample_rate if hasattr(config, 'trace') else 0,
                "analytics_enabled": config.analytics_enabled if hasattr(config, 'analytics_enabled') else False
            }
        }


# Global initializer instance
_datadog_apm_initializer: Optional[DataDogAPMInitializer] = None


async def initialize_datadog_apm(service_name: Optional[str] = None) -> bool:
    """
    Initialize DataDog APM for the application
    
    Args:
        service_name: Override service name
        
    Returns:
        True if successful, False otherwise
    """
    global _datadog_apm_initializer
    
    if _datadog_apm_initializer is None:
        _datadog_apm_initializer = DataDogAPMInitializer()
    
    return await _datadog_apm_initializer.initialize_datadog_apm(service_name)


async def shutdown_datadog_apm():
    """Shutdown DataDog APM gracefully."""
    global _datadog_apm_initializer
    
    if _datadog_apm_initializer:
        await _datadog_apm_initializer.shutdown_datadog_apm()


def get_datadog_apm_initializer() -> Optional[DataDogAPMInitializer]:
    """Get the DataDog APM initializer instance."""
    return _datadog_apm_initializer


def get_datadog_apm_status() -> Dict[str, Any]:
    """Get DataDog APM initialization status."""
    global _datadog_apm_initializer
    
    if _datadog_apm_initializer:
        return _datadog_apm_initializer.get_initialization_status()
    else:
        return {
            "is_initialized": False,
            "error": "DataDog APM not initialized"
        }


# Context manager for DataDog APM lifecycle
class DataDogAPMContext:
    """Context manager for DataDog APM lifecycle management."""
    
    def __init__(self, service_name: Optional[str] = None):
        self.service_name = service_name
        self.initialization_successful = False
    
    async def __aenter__(self):
        self.initialization_successful = await initialize_datadog_apm(self.service_name)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.initialization_successful:
            await shutdown_datadog_apm()


# Startup hook for FastAPI or other ASGI applications
async def datadog_apm_startup_hook(service_name: Optional[str] = None):
    """Startup hook for integrating DataDog APM with ASGI applications."""
    
    logger.info("DataDog APM startup hook called")
    success = await initialize_datadog_apm(service_name)
    
    if not success:
        logger.error("DataDog APM initialization failed during startup")
        # Decide whether to continue or fail based on configuration
        if os.getenv("DD_APM_REQUIRED", "false").lower() == "true":
            raise RuntimeError("DataDog APM initialization required but failed")
    
    return success


async def datadog_apm_shutdown_hook():
    """Shutdown hook for integrating DataDog APM with ASGI applications."""
    
    logger.info("DataDog APM shutdown hook called")
    await shutdown_datadog_apm()