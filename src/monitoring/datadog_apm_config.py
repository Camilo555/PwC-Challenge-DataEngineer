"""
Comprehensive DataDog APM Configuration Management
Provides centralized configuration, initialization, and management for DataDog APM across the platform
"""

import os
import json
import yaml
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, field, asdict
from pathlib import Path
from enum import Enum

from ddtrace import config
from ddtrace.filters import FilterRequestsOnUrl

from core.config import get_settings
from core.logging import get_logger

logger = get_logger(__name__)
settings = get_settings()


class Environment(Enum):
    """Deployment environments."""
    DEVELOPMENT = "development"
    STAGING = "staging"
    PRODUCTION = "production"
    TESTING = "testing"


class LogLevel(Enum):
    """DataDog trace log levels."""
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"


@dataclass
class SamplingRule:
    """DataDog sampling rule configuration."""
    service: Optional[str] = None
    operation: Optional[str] = None
    resource: Optional[str] = None
    tags: Optional[Dict[str, str]] = None
    sample_rate: float = 1.0
    max_per_second: Optional[int] = None


@dataclass
class ServiceConfig:
    """Service-specific APM configuration."""
    service_name: str
    version: str = "1.0.0"
    env: str = "development"
    
    # Tracing configuration
    trace_enabled: bool = True
    trace_sample_rate: float = 1.0
    trace_analytics_enabled: bool = True
    
    # Profiling configuration
    profiling_enabled: bool = False
    profiling_upload_period: int = 60  # seconds
    profiling_max_frames: int = 64
    
    # Runtime metrics
    runtime_metrics_enabled: bool = True
    
    # Logs injection
    logs_injection: bool = True
    
    # Custom tags
    global_tags: Dict[str, str] = field(default_factory=dict)
    
    # Instrumentation settings
    auto_instrument: bool = True
    instrument_libraries: List[str] = field(default_factory=lambda: [
        "fastapi", "sqlalchemy", "requests", "httpx", "psycopg2", "kafka", "pika"
    ])
    
    # Filters
    ignored_resources: List[str] = field(default_factory=lambda: [
        r"GET /health.*",
        r"GET /metrics.*",
        r"GET /docs.*",
        r"GET /redoc.*",
        r"GET /openapi.json.*",
        r"GET /favicon.ico.*"
    ])
    
    # Sampling rules
    sampling_rules: List[SamplingRule] = field(default_factory=list)


@dataclass
class AgentConfig:
    """DataDog agent configuration."""
    hostname: str = "localhost"
    port: int = 8126
    https: bool = False
    timeout: float = 2.0
    
    # Agent health check
    health_check_interval: int = 60  # seconds
    
    # UDS (Unix Domain Socket) configuration
    uds_path: Optional[str] = None


@dataclass
class ProfilingConfig:
    """DataDog profiling configuration."""
    enabled: bool = False
    
    # Upload settings
    upload_period: int = 60  # seconds
    max_time_usage_pct: int = 1  # Max 1% CPU usage for profiling
    
    # Profiling types
    cpu_enabled: bool = True
    memory_enabled: bool = True
    exception_enabled: bool = True
    lock_enabled: bool = True
    
    # Stack trace settings
    max_frames: int = 64
    capture_pct: int = 5  # Capture 5% of operations
    
    # Export settings
    export_libdd_enabled: bool = False
    export_http_enabled: bool = True


@dataclass
class LogsConfig:
    """DataDog logs configuration."""
    injection_enabled: bool = True
    
    # Log correlation
    trace_id_injection: bool = True
    span_id_injection: bool = True
    
    # Log collection
    collect_logs: bool = False  # Set to True if using DataDog log agent
    log_level: LogLevel = LogLevel.INFO
    
    # Custom log attributes
    custom_attributes: Dict[str, str] = field(default_factory=dict)


@dataclass
class SecurityConfig:
    """DataDog security and compliance configuration."""
    # Data scrubbing
    obfuscate_sql_values: bool = True
    obfuscate_redis_commands: bool = True
    obfuscate_http_headers: List[str] = field(default_factory=lambda: [
        "authorization", "x-api-key", "cookie", "set-cookie"
    ])
    
    # PII protection
    scrub_sensitive_data: bool = True
    sensitive_keys: List[str] = field(default_factory=lambda: [
        "password", "secret", "key", "token", "auth", "credential",
        "ssn", "credit_card", "phone", "email"
    ])
    
    # Compliance
    enable_pci_compliance: bool = False
    enable_hipaa_compliance: bool = False
    enable_gdpr_compliance: bool = True


@dataclass
class PerformanceConfig:
    """Performance and optimization configuration."""
    # Buffer settings
    trace_buffer_size: int = 1000
    flush_interval: float = 1.0  # seconds
    
    # Span limits
    max_spans_per_trace: int = 100000
    max_trace_duration: int = 3600  # seconds
    
    # Memory management
    max_memory_usage_mb: int = 512
    partial_flush_enabled: bool = True
    partial_flush_min_spans: int = 300
    
    # Network optimization
    compression_enabled: bool = True
    retry_attempts: int = 3
    retry_delay: float = 1.0


@dataclass
class DataDogAPMConfiguration:
    """Complete DataDog APM configuration."""
    # Core configuration
    api_key: str = ""
    service: ServiceConfig = field(default_factory=lambda: ServiceConfig("default-service"))
    
    # Component configurations
    agent: AgentConfig = field(default_factory=AgentConfig)
    profiling: ProfilingConfig = field(default_factory=ProfilingConfig)
    logs: LogsConfig = field(default_factory=LogsConfig)
    security: SecurityConfig = field(default_factory=SecurityConfig)
    performance: PerformanceConfig = field(default_factory=PerformanceConfig)
    
    # Environment-specific settings
    environment: Environment = Environment.DEVELOPMENT
    debug_mode: bool = False
    
    # Feature flags
    enable_startup_logs: bool = True
    enable_shutdown_hooks: bool = True
    enable_health_checks: bool = True


class DataDogAPMConfigManager:
    """
    Comprehensive DataDog APM configuration manager
    
    Features:
    - Environment-specific configuration loading
    - Dynamic configuration updates
    - Configuration validation
    - Service discovery integration
    - Multi-service configuration management
    - Configuration templates and presets
    """
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self._configurations: Dict[str, DataDogAPMConfiguration] = {}
        self._active_configuration: Optional[DataDogAPMConfiguration] = None
        self._config_file_path: Optional[Path] = None
        
        # Load configuration from environment and files
        self._load_configuration()
    
    def _load_configuration(self):
        """Load configuration from various sources."""
        
        try:
            # 1. Load from environment variables
            env_config = self._load_from_environment()
            
            # 2. Load from configuration files
            file_configs = self._load_from_files()
            
            # 3. Merge configurations (env overrides file)
            for service_name, file_config in file_configs.items():
                merged_config = self._merge_configurations(file_config, env_config)
                self._configurations[service_name] = merged_config
            
            # 4. Set default configuration if none exists
            if not self._configurations:
                self._configurations["default"] = env_config
            
            # 5. Set active configuration
            service_name = os.getenv("DD_SERVICE", "default")
            self._active_configuration = self._configurations.get(service_name, env_config)
            
            self.logger.info(f"Loaded DataDog APM configuration for {len(self._configurations)} services")
            
        except Exception as e:
            self.logger.error(f"Failed to load DataDog APM configuration: {str(e)}")
            # Fallback to minimal configuration
            self._active_configuration = DataDogAPMConfiguration()
    
    def _load_from_environment(self) -> DataDogAPMConfiguration:
        """Load configuration from environment variables."""
        
        # Determine environment
        env_name = os.getenv("DD_ENV", "development").lower()
        try:
            environment = Environment(env_name)
        except ValueError:
            environment = Environment.DEVELOPMENT
            self.logger.warning(f"Unknown environment '{env_name}', defaulting to development")
        
        # Service configuration
        service_config = ServiceConfig(
            service_name=os.getenv("DD_SERVICE", "pwc-data-engineering"),
            version=os.getenv("DD_VERSION", "1.0.0"),
            env=environment.value,
            trace_enabled=os.getenv("DD_TRACE_ENABLED", "true").lower() == "true",
            trace_sample_rate=float(os.getenv("DD_TRACE_SAMPLE_RATE", "1.0")),
            trace_analytics_enabled=os.getenv("DD_TRACE_ANALYTICS_ENABLED", "true").lower() == "true",
            profiling_enabled=os.getenv("DD_PROFILING_ENABLED", "false").lower() == "true",
            runtime_metrics_enabled=os.getenv("DD_RUNTIME_METRICS_ENABLED", "true").lower() == "true",
            logs_injection=os.getenv("DD_LOGS_INJECTION", "true").lower() == "true"
        )
        
        # Global tags from environment
        tags_env = os.getenv("DD_TAGS", "")
        if tags_env:
            tags = {}
            for tag_pair in tags_env.split(","):
                if ":" in tag_pair:
                    key, value = tag_pair.split(":", 1)
                    tags[key.strip()] = value.strip()
            service_config.global_tags = tags
        
        # Agent configuration
        agent_config = AgentConfig(
            hostname=os.getenv("DD_AGENT_HOST", "localhost"),
            port=int(os.getenv("DD_TRACE_AGENT_PORT", "8126")),
            https=os.getenv("DD_TRACE_AGENT_HTTPS", "false").lower() == "true",
            timeout=float(os.getenv("DD_TRACE_AGENT_TIMEOUT", "2.0")),
            uds_path=os.getenv("DD_TRACE_AGENT_UDS")
        )
        
        # Profiling configuration
        profiling_config = ProfilingConfig(
            enabled=service_config.profiling_enabled,
            upload_period=int(os.getenv("DD_PROFILING_UPLOAD_PERIOD", "60")),
            max_frames=int(os.getenv("DD_PROFILING_MAX_FRAMES", "64")),
            cpu_enabled=os.getenv("DD_PROFILING_ENABLE_CPU", "true").lower() == "true",
            memory_enabled=os.getenv("DD_PROFILING_ENABLE_MEMORY", "true").lower() == "true"
        )
        
        # Security configuration based on environment
        security_config = SecurityConfig(
            obfuscate_sql_values=environment != Environment.DEVELOPMENT,
            scrub_sensitive_data=environment == Environment.PRODUCTION,
            enable_gdpr_compliance=environment == Environment.PRODUCTION
        )
        
        # Performance configuration based on environment
        if environment == Environment.PRODUCTION:
            performance_config = PerformanceConfig(
                trace_buffer_size=2000,
                max_memory_usage_mb=1024,
                compression_enabled=True
            )
        else:
            performance_config = PerformanceConfig()
        
        return DataDogAPMConfiguration(
            api_key=os.getenv("DD_API_KEY", ""),
            service=service_config,
            agent=agent_config,
            profiling=profiling_config,
            security=security_config,
            performance=performance_config,
            environment=environment,
            debug_mode=os.getenv("DD_DEBUG", "false").lower() == "true"
        )
    
    def _load_from_files(self) -> Dict[str, DataDogAPMConfiguration]:
        """Load configurations from YAML/JSON files."""
        
        configurations = {}
        
        # Look for configuration files
        config_paths = [
            Path("config/datadog-apm.yaml"),
            Path("config/datadog-apm.yml"),
            Path("config/datadog-apm.json"),
            Path("datadog-apm.yaml"),
            Path("datadog-apm.yml"),
            Path("datadog-apm.json")
        ]
        
        for config_path in config_paths:
            if config_path.exists():
                try:
                    configurations.update(self._parse_config_file(config_path))
                    self._config_file_path = config_path
                    self.logger.info(f"Loaded DataDog APM configuration from {config_path}")
                    break
                except Exception as e:
                    self.logger.warning(f"Failed to parse config file {config_path}: {str(e)}")
        
        return configurations
    
    def _parse_config_file(self, config_path: Path) -> Dict[str, DataDogAPMConfiguration]:
        """Parse configuration file (YAML or JSON)."""
        
        with open(config_path, 'r') as f:
            if config_path.suffix.lower() in ['.yaml', '.yml']:
                config_data = yaml.safe_load(f)
            else:
                config_data = json.load(f)
        
        configurations = {}
        
        # Handle single service configuration
        if 'services' not in config_data:
            config_data = {'services': {'default': config_data}}
        
        for service_name, service_config in config_data.get('services', {}).items():
            try:
                configurations[service_name] = self._parse_service_config(service_config)
            except Exception as e:
                self.logger.error(f"Failed to parse configuration for service {service_name}: {str(e)}")
        
        return configurations
    
    def _parse_service_config(self, config_data: Dict[str, Any]) -> DataDogAPMConfiguration:
        """Parse service configuration from dictionary."""
        
        # Service configuration
        service_data = config_data.get('service', {})
        service_config = ServiceConfig(
            service_name=service_data.get('name', 'default-service'),
            version=service_data.get('version', '1.0.0'),
            env=service_data.get('env', 'development'),
            trace_enabled=service_data.get('trace_enabled', True),
            trace_sample_rate=service_data.get('trace_sample_rate', 1.0),
            trace_analytics_enabled=service_data.get('trace_analytics_enabled', True),
            profiling_enabled=service_data.get('profiling_enabled', False),
            runtime_metrics_enabled=service_data.get('runtime_metrics_enabled', True),
            logs_injection=service_data.get('logs_injection', True),
            global_tags=service_data.get('global_tags', {}),
            auto_instrument=service_data.get('auto_instrument', True),
            instrument_libraries=service_data.get('instrument_libraries', []),
            ignored_resources=service_data.get('ignored_resources', [])
        )
        
        # Sampling rules
        if 'sampling_rules' in service_data:
            service_config.sampling_rules = [
                SamplingRule(**rule) for rule in service_data['sampling_rules']
            ]
        
        # Agent configuration
        agent_data = config_data.get('agent', {})
        agent_config = AgentConfig(
            hostname=agent_data.get('hostname', 'localhost'),
            port=agent_data.get('port', 8126),
            https=agent_data.get('https', False),
            timeout=agent_data.get('timeout', 2.0),
            uds_path=agent_data.get('uds_path')
        )
        
        # Other configurations
        profiling_config = ProfilingConfig(**config_data.get('profiling', {}))
        logs_config = LogsConfig(**config_data.get('logs', {}))
        security_config = SecurityConfig(**config_data.get('security', {}))
        performance_config = PerformanceConfig(**config_data.get('performance', {}))
        
        # Determine environment
        env_name = service_config.env.lower()
        try:
            environment = Environment(env_name)
        except ValueError:
            environment = Environment.DEVELOPMENT
        
        return DataDogAPMConfiguration(
            api_key=config_data.get('api_key', ''),
            service=service_config,
            agent=agent_config,
            profiling=profiling_config,
            logs=logs_config,
            security=security_config,
            performance=performance_config,
            environment=environment,
            debug_mode=config_data.get('debug_mode', False)
        )
    
    def _merge_configurations(self, file_config: DataDogAPMConfiguration, 
                            env_config: DataDogAPMConfiguration) -> DataDogAPMConfiguration:
        """Merge file and environment configurations (env takes precedence)."""
        
        # Create a copy of file config
        merged = DataDogAPMConfiguration(**asdict(file_config))
        
        # Override with environment values where they exist
        if env_config.api_key:
            merged.api_key = env_config.api_key
        
        # Merge service configuration
        if env_config.service.service_name != "pwc-data-engineering":  # Default check
            merged.service.service_name = env_config.service.service_name
        if env_config.service.version != "1.0.0":  # Default check
            merged.service.version = env_config.service.version
        
        # Environment always comes from env vars
        merged.environment = env_config.environment
        merged.service.env = env_config.service.env
        
        # Merge global tags
        merged.service.global_tags.update(env_config.service.global_tags)
        
        # Agent configuration from environment takes precedence
        if env_config.agent.hostname != "localhost":
            merged.agent.hostname = env_config.agent.hostname
        if env_config.agent.port != 8126:
            merged.agent.port = env_config.agent.port
        
        return merged
    
    # Public API
    
    def get_configuration(self, service_name: Optional[str] = None) -> DataDogAPMConfiguration:
        """Get configuration for a service."""
        
        if service_name and service_name in self._configurations:
            return self._configurations[service_name]
        
        if self._active_configuration:
            return self._active_configuration
        
        # Fallback
        return DataDogAPMConfiguration()
    
    def get_active_configuration(self) -> DataDogAPMConfiguration:
        """Get the currently active configuration."""
        
        if self._active_configuration:
            return self._active_configuration
        
        return DataDogAPMConfiguration()
    
    def set_active_service(self, service_name: str) -> bool:
        """Set the active service configuration."""
        
        if service_name in self._configurations:
            self._active_configuration = self._configurations[service_name]
            self.logger.info(f"Switched to configuration for service: {service_name}")
            return True
        
        self.logger.warning(f"No configuration found for service: {service_name}")
        return False
    
    def register_service_configuration(self, service_name: str, 
                                     configuration: DataDogAPMConfiguration):
        """Register a new service configuration."""
        
        self._configurations[service_name] = configuration
        self.logger.info(f"Registered configuration for service: {service_name}")
    
    def apply_configuration(self, configuration: Optional[DataDogAPMConfiguration] = None):
        """Apply DataDog configuration to the tracer."""
        
        if configuration is None:
            configuration = self.get_active_configuration()
        
        try:
            # Set basic configuration
            config.service = configuration.service.service_name
            config.env = configuration.service.env
            config.version = configuration.service.version
            
            # Configure agent
            config.trace.hostname = configuration.agent.hostname
            config.trace.port = configuration.agent.port
            
            # Configure sampling
            config.trace.sample_rate = configuration.service.trace_sample_rate
            
            # Configure analytics
            config.analytics_enabled = configuration.service.trace_analytics_enabled
            
            # Configure profiling
            if configuration.profiling.enabled:
                config.profiling.enabled = True
                config.profiling.upload_period = configuration.profiling.upload_period
                config.profiling.max_frames = configuration.profiling.max_frames
            
            # Configure runtime metrics
            config.runtime_metrics.enabled = configuration.service.runtime_metrics_enabled
            
            # Configure logs injection
            config.logs_injection = configuration.service.logs_injection
            
            # Set global tags
            for key, value in configuration.service.global_tags.items():
                config.tags[key] = value
            
            # Configure security settings
            if configuration.security.obfuscate_sql_values:
                config.database_query_obfuscation = True
            
            self.logger.info(f"Applied DataDog configuration for service: {configuration.service.service_name}")
            
        except Exception as e:
            self.logger.error(f"Failed to apply DataDog configuration: {str(e)}")
            raise
    
    def validate_configuration(self, configuration: Optional[DataDogAPMConfiguration] = None) -> Dict[str, Any]:
        """Validate DataDog configuration."""
        
        if configuration is None:
            configuration = self.get_active_configuration()
        
        validation_result = {
            "valid": True,
            "warnings": [],
            "errors": []
        }
        
        # Check required fields
        if not configuration.api_key and configuration.environment == Environment.PRODUCTION:
            validation_result["errors"].append("API key is required for production environment")
            validation_result["valid"] = False
        
        if not configuration.service.service_name:
            validation_result["errors"].append("Service name is required")
            validation_result["valid"] = False
        
        # Check sampling rate
        if not (0.0 <= configuration.service.trace_sample_rate <= 1.0):
            validation_result["errors"].append("Trace sample rate must be between 0.0 and 1.0")
            validation_result["valid"] = False
        
        # Check profiling settings
        if configuration.profiling.enabled and configuration.profiling.max_time_usage_pct > 10:
            validation_result["warnings"].append("High profiling CPU usage may impact performance")
        
        # Check performance settings
        if configuration.performance.max_spans_per_trace > 50000:
            validation_result["warnings"].append("High max spans per trace may cause memory issues")
        
        # Check agent connectivity
        if configuration.agent.hostname == "localhost" and configuration.environment == Environment.PRODUCTION:
            validation_result["warnings"].append("Using localhost agent hostname in production")
        
        return validation_result
    
    def export_configuration(self, service_name: Optional[str] = None, 
                           format: str = "yaml") -> str:
        """Export configuration to YAML or JSON format."""
        
        config = self.get_configuration(service_name)
        config_dict = asdict(config)
        
        if format.lower() == "json":
            return json.dumps(config_dict, indent=2, default=str)
        else:
            return yaml.dump(config_dict, default_flow_style=False, indent=2)
    
    def get_configuration_summary(self) -> Dict[str, Any]:
        """Get summary of all configurations."""
        
        summary = {
            "total_services": len(self._configurations),
            "active_service": self._active_configuration.service.service_name if self._active_configuration else None,
            "config_file_path": str(self._config_file_path) if self._config_file_path else None,
            "services": {}
        }
        
        for service_name, config in self._configurations.items():
            summary["services"][service_name] = {
                "service_name": config.service.service_name,
                "version": config.service.version,
                "environment": config.environment.value,
                "trace_enabled": config.service.trace_enabled,
                "profiling_enabled": config.profiling.enabled,
                "agent_host": config.agent.hostname,
                "agent_port": config.agent.port
            }
        
        return summary


# Global configuration manager instance
_config_manager: Optional[DataDogAPMConfigManager] = None


def get_datadog_apm_config_manager() -> DataDogAPMConfigManager:
    """Get the global DataDog APM configuration manager."""
    global _config_manager
    
    if _config_manager is None:
        _config_manager = DataDogAPMConfigManager()
    
    return _config_manager


def get_datadog_apm_configuration(service_name: Optional[str] = None) -> DataDogAPMConfiguration:
    """Get DataDog APM configuration for a service."""
    manager = get_datadog_apm_config_manager()
    return manager.get_configuration(service_name)


def apply_datadog_apm_configuration(service_name: Optional[str] = None):
    """Apply DataDog APM configuration."""
    manager = get_datadog_apm_config_manager()
    config = manager.get_configuration(service_name)
    manager.apply_configuration(config)


def validate_datadog_apm_configuration(service_name: Optional[str] = None) -> Dict[str, Any]:
    """Validate DataDog APM configuration."""
    manager = get_datadog_apm_config_manager()
    config = manager.get_configuration(service_name)
    return manager.validate_configuration(config)


# Configuration templates and presets
class DataDogAPMPresets:
    """Predefined configuration presets for common scenarios."""
    
    @staticmethod
    def development_preset() -> DataDogAPMConfiguration:
        """Configuration preset for development environment."""
        return DataDogAPMConfiguration(
            service=ServiceConfig(
                service_name="dev-service",
                env="development",
                trace_sample_rate=1.0,
                profiling_enabled=True,
                debug_mode=True
            ),
            environment=Environment.DEVELOPMENT,
            security=SecurityConfig(
                obfuscate_sql_values=False,
                scrub_sensitive_data=False
            ),
            debug_mode=True
        )
    
    @staticmethod
    def production_preset() -> DataDogAPMConfiguration:
        """Configuration preset for production environment."""
        return DataDogAPMConfiguration(
            service=ServiceConfig(
                service_name="prod-service",
                env="production",
                trace_sample_rate=0.1,  # Lower sampling for production
                profiling_enabled=True,
                profiling_upload_period=300  # Less frequent uploads
            ),
            environment=Environment.PRODUCTION,
            security=SecurityConfig(
                obfuscate_sql_values=True,
                scrub_sensitive_data=True,
                enable_gdpr_compliance=True,
                enable_pci_compliance=True
            ),
            performance=PerformanceConfig(
                trace_buffer_size=2000,
                max_memory_usage_mb=1024,
                compression_enabled=True
            ),
            debug_mode=False
        )
    
    @staticmethod
    def high_performance_preset() -> DataDogAPMConfiguration:
        """Configuration preset for high-performance scenarios."""
        return DataDogAPMConfiguration(
            service=ServiceConfig(
                service_name="high-perf-service",
                trace_sample_rate=0.01,  # Very low sampling
                profiling_enabled=False,  # Disable profiling for max performance
                trace_analytics_enabled=False
            ),
            performance=PerformanceConfig(
                trace_buffer_size=5000,
                flush_interval=0.1,  # Frequent flushes
                partial_flush_enabled=True,
                partial_flush_min_spans=100,
                compression_enabled=True
            )
        )
    
    @staticmethod
    def debugging_preset() -> DataDogAPMConfiguration:
        """Configuration preset for debugging scenarios."""
        return DataDogAPMConfiguration(
            service=ServiceConfig(
                service_name="debug-service",
                trace_sample_rate=1.0,  # Full sampling
                trace_analytics_enabled=True
            ),
            performance=PerformanceConfig(
                max_spans_per_trace=200000,  # Allow many spans for debugging
                flush_interval=5.0  # Less frequent flushes for debugging
            ),
            debug_mode=True,
            enable_startup_logs=True
        )