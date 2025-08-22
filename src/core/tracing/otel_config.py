"""
OpenTelemetry Configuration
Provides centralized configuration for OpenTelemetry tracing infrastructure.
"""

import os
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
from enum import Enum

try:
    from opentelemetry import trace
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor, ConsoleSpanExporter
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.exporter.jaeger.thrift import JaegerExporter
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.propagators.composite import CompositePropagator
    from opentelemetry.propagators.b3 import B3Format
    from opentelemetry.propagators.jaeger import JaegerPropagator  
    from opentelemetry.trace.propagation.tracecontext import TraceContextTextMapPropagator
    OTEL_AVAILABLE = True
except ImportError as e:
    print(f"OpenTelemetry import failed: {e}")
    OTEL_AVAILABLE = False
    TracerProvider = None
    Resource = None
    BatchSpanProcessor = None
    ConsoleSpanExporter = None
    CompositePropagator = None

from core.logging import get_logger

logger = get_logger(__name__)


class TracingBackend(str, Enum):
    """Supported tracing backends."""
    CONSOLE = "console"
    JAEGER = "jaeger"
    OTLP = "otlp"
    ZIPKIN = "zipkin"
    DATADOG = "datadog"


@dataclass
class TracingConfig:
    """Configuration for OpenTelemetry tracing."""
    
    # Core settings
    enabled: bool = True
    service_name: str = "retail-etl-pipeline"
    service_version: str = "1.0.0"
    environment: str = "development"
    
    # Backend configuration
    backend: TracingBackend = TracingBackend.CONSOLE
    endpoint: Optional[str] = None
    headers: Optional[Dict[str, str]] = None
    
    # Sampling configuration
    sampling_rate: float = 1.0  # 100% sampling for development
    
    # Export configuration
    export_timeout_seconds: int = 30
    export_max_batch_size: int = 512
    export_schedule_delay_millis: int = 5000
    
    # Resource attributes
    resource_attributes: Optional[Dict[str, str]] = None
    
    # Instrumentation
    auto_instrument: bool = True
    instrument_db: bool = True
    instrument_http: bool = True
    instrument_redis: bool = True
    
    # Propagation
    propagators: List[str] = None
    
    @classmethod
    def from_environment(cls) -> 'TracingConfig':
        """Create configuration from environment variables."""
        
        # Default propagators
        default_propagators = ["tracecontext", "baggage", "b3", "jaeger"]
        
        return cls(
            enabled=os.getenv('OTEL_ENABLED', 'true').lower() in ('true', '1', 'yes'),
            service_name=os.getenv('OTEL_SERVICE_NAME', 'retail-etl-pipeline'),
            service_version=os.getenv('OTEL_SERVICE_VERSION', '1.0.0'),
            environment=os.getenv('ENVIRONMENT', 'development'),
            
            backend=TracingBackend(os.getenv('OTEL_BACKEND', 'console')),
            endpoint=os.getenv('OTEL_EXPORTER_ENDPOINT'),
            
            sampling_rate=float(os.getenv('OTEL_SAMPLING_RATE', '1.0')),
            
            export_timeout_seconds=int(os.getenv('OTEL_EXPORT_TIMEOUT', '30')),
            export_max_batch_size=int(os.getenv('OTEL_EXPORT_BATCH_SIZE', '512')),
            export_schedule_delay_millis=int(os.getenv('OTEL_EXPORT_DELAY', '5000')),
            
            auto_instrument=os.getenv('OTEL_AUTO_INSTRUMENT', 'true').lower() in ('true', '1', 'yes'),
            instrument_db=os.getenv('OTEL_INSTRUMENT_DB', 'true').lower() in ('true', '1', 'yes'),
            instrument_http=os.getenv('OTEL_INSTRUMENT_HTTP', 'true').lower() in ('true', '1', 'yes'),
            instrument_redis=os.getenv('OTEL_INSTRUMENT_REDIS', 'true').lower() in ('true', '1', 'yes'),
            
            propagators=os.getenv('OTEL_PROPAGATORS', ','.join(default_propagators)).split(','),
            
            resource_attributes=_parse_resource_attributes()
        )


def _parse_resource_attributes() -> Dict[str, str]:
    """Parse resource attributes from environment variables."""
    attributes = {}
    
    # Parse OTEL_RESOURCE_ATTRIBUTES format: key1=value1,key2=value2
    resource_attrs = os.getenv('OTEL_RESOURCE_ATTRIBUTES', '')
    if resource_attrs:
        for pair in resource_attrs.split(','):
            if '=' in pair:
                key, value = pair.split('=', 1)
                attributes[key.strip()] = value.strip()
    
    # Add default attributes
    attributes.update({
        'service.name': os.getenv('OTEL_SERVICE_NAME', 'retail-etl-pipeline'),
        'service.version': os.getenv('OTEL_SERVICE_VERSION', '1.0.0'),
        'deployment.environment': os.getenv('ENVIRONMENT', 'development'),
        'service.namespace': 'retail-analytics',
        'service.instance.id': os.getenv('HOSTNAME', 'localhost')
    })
    
    return attributes


class OpenTelemetryConfigurator:
    """Handles OpenTelemetry setup and configuration."""
    
    def __init__(self, config: TracingConfig):
        self.config = config
        self._tracer_provider: Optional[TracerProvider] = None
        
    def configure(self) -> bool:
        """Configure OpenTelemetry tracing."""
        if not OTEL_AVAILABLE:
            logger.warning("OpenTelemetry not available, tracing disabled")
            return False
            
        if not self.config.enabled:
            logger.info("Tracing disabled by configuration")
            return False
        
        try:
            # Create resource
            resource = self._create_resource()
            
            # Create tracer provider
            self._tracer_provider = TracerProvider(resource=resource)
            
            # Add span processors
            self._add_span_processors()
            
            # Set global tracer provider
            trace.set_tracer_provider(self._tracer_provider)
            
            # Configure propagators
            self._configure_propagators()
            
            logger.info(f"OpenTelemetry configured with {self.config.backend} backend")
            return True
            
        except Exception as e:
            logger.error(f"Failed to configure OpenTelemetry: {e}")
            return False
    
    def _create_resource(self) -> Resource:
        """Create OpenTelemetry resource with attributes."""
        attributes = self.config.resource_attributes or {}
        return Resource.create(attributes)
    
    def _add_span_processors(self) -> None:
        """Add span processors based on backend configuration."""
        exporter = self._create_exporter()
        if exporter:
            processor = BatchSpanProcessor(
                exporter,
                max_export_batch_size=self.config.export_max_batch_size,
                schedule_delay_millis=self.config.export_schedule_delay_millis,
                export_timeout_millis=self.config.export_timeout_seconds * 1000
            )
            self._tracer_provider.add_span_processor(processor)
    
    def _create_exporter(self):
        """Create span exporter based on backend configuration."""
        backend = self.config.backend
        
        if backend == TracingBackend.CONSOLE:
            return ConsoleSpanExporter()
            
        elif backend == TracingBackend.JAEGER:
            return JaegerExporter(
                agent_host_name=self._get_host_from_endpoint(),
                agent_port=self._get_port_from_endpoint(14268),
                collector_endpoint=self.config.endpoint
            )
            
        elif backend == TracingBackend.OTLP:
            return OTLPSpanExporter(
                endpoint=self.config.endpoint or "http://localhost:4317",
                headers=self.config.headers
            )
            
        else:
            logger.warning(f"Unsupported backend: {backend}, using console")
            return ConsoleSpanExporter()
    
    def _get_host_from_endpoint(self) -> str:
        """Extract host from endpoint URL."""
        if not self.config.endpoint:
            return "localhost"
        
        from urllib.parse import urlparse
        parsed = urlparse(self.config.endpoint)
        return parsed.hostname or "localhost"
    
    def _get_port_from_endpoint(self, default: int) -> int:
        """Extract port from endpoint URL."""
        if not self.config.endpoint:
            return default
            
        from urllib.parse import urlparse
        parsed = urlparse(self.config.endpoint)
        return parsed.port or default
    
    def _configure_propagators(self) -> None:
        """Configure trace context propagators."""
        if not OTEL_AVAILABLE:
            return
        
        propagators = []
        
        for prop_name in self.config.propagators:
            prop_name = prop_name.strip().lower()
            
            if prop_name == "tracecontext":
                propagators.append(TraceContextTextMapPropagator())
            elif prop_name == "b3":
                propagators.append(B3Format())
            elif prop_name == "jaeger":
                propagators.append(JaegerPropagator())
        
        if propagators:
            from opentelemetry import propagate
            propagate.set_global_textmap(CompositePropagator(propagators))
    
    def shutdown(self) -> None:
        """Shutdown tracing infrastructure."""
        if self._tracer_provider:
            self._tracer_provider.shutdown()
            logger.info("OpenTelemetry tracing shut down")


# Global configurator instance
_configurator: Optional[OpenTelemetryConfigurator] = None
_config: Optional[TracingConfig] = None


def configure_opentelemetry(config: Optional[TracingConfig] = None) -> bool:
    """
    Configure OpenTelemetry tracing globally.
    
    Args:
        config: Tracing configuration, or None to use environment
        
    Returns:
        True if configuration succeeded
    """
    global _configurator, _config
    
    _config = config or TracingConfig.from_environment()
    _configurator = OpenTelemetryConfigurator(_config)
    
    return _configurator.configure()


def get_otel_config() -> Optional[TracingConfig]:
    """Get current OpenTelemetry configuration."""
    return _config


def shutdown_tracing() -> None:
    """Shutdown OpenTelemetry tracing."""
    global _configurator
    
    if _configurator:
        _configurator.shutdown()
        _configurator = None


def is_tracing_enabled() -> bool:
    """Check if tracing is enabled and configured."""
    return _config is not None and _config.enabled and OTEL_AVAILABLE


# Startup hook for automatic configuration
def auto_configure_tracing() -> bool:
    """Automatically configure tracing from environment."""
    if os.getenv('OTEL_AUTO_CONFIGURE', 'true').lower() in ('true', '1', 'yes'):
        return configure_opentelemetry()
    return False