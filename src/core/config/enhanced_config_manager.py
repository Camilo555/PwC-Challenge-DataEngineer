"""
Enhanced Configuration Management System
=======================================

Advanced configuration management with features:
- Hot reloading
- Environment-specific overrides
- Configuration validation
- Secrets management integration
- Configuration versioning and rollback
- Performance optimization settings
"""
from __future__ import annotations

import asyncio
import json
import os
import threading
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Type, TypeVar, Union, Callable
import hashlib

import yaml
from pydantic import BaseModel, Field, validator
from pydantic_settings import BaseSettings
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

import logging
from core.security.secret_manager import SecretManager, SecretConfig

logger = logging.getLogger(__name__)

T = TypeVar('T', bound=BaseSettings)


class ConfigFormat(str, Enum):
    """Configuration file formats."""
    YAML = "yaml"
    JSON = "json"
    ENV = "env"
    TOML = "toml"


class ConfigScope(str, Enum):
    """Configuration scope levels."""
    GLOBAL = "global"
    APPLICATION = "application"
    SERVICE = "service"
    ENVIRONMENT = "environment"
    FEATURE = "feature"


class ConfigPriority(int, Enum):
    """Configuration priority levels."""
    DEFAULT = 0
    CONFIG_FILE = 100
    ENVIRONMENT_VARS = 200
    COMMAND_LINE = 300
    RUNTIME_OVERRIDE = 400
    EMERGENCY_OVERRIDE = 500


@dataclass
class ConfigSource:
    """Configuration source metadata."""
    path: Optional[Path] = None
    format: ConfigFormat = ConfigFormat.YAML
    priority: ConfigPriority = ConfigPriority.CONFIG_FILE
    scope: ConfigScope = ConfigScope.APPLICATION
    last_modified: Optional[datetime] = None
    checksum: Optional[str] = None
    is_encrypted: bool = False
    
    def calculate_checksum(self, data: Dict[str, Any]) -> str:
        """Calculate checksum of configuration data."""
        content = json.dumps(data, sort_keys=True)
        return hashlib.sha256(content.encode()).hexdigest()


class PerformanceSettings(BaseModel):
    """Performance optimization settings."""
    
    # Database settings
    db_pool_size: int = Field(default=20, ge=1, le=100)
    db_max_overflow: int = Field(default=30, ge=0, le=100)
    db_pool_timeout: int = Field(default=30, ge=1, le=300)
    db_pool_recycle: int = Field(default=3600, ge=300, le=7200)
    
    # Caching settings
    cache_default_ttl: int = Field(default=300, ge=60, le=3600)
    cache_max_size: int = Field(default=1000, ge=100, le=10000)
    cache_cleanup_interval: int = Field(default=60, ge=30, le=300)
    
    # API settings
    api_timeout: int = Field(default=30, ge=1, le=300)
    api_max_retries: int = Field(default=3, ge=0, le=10)
    api_backoff_factor: float = Field(default=0.5, ge=0.1, le=2.0)
    api_compression_threshold: int = Field(default=1024, ge=512, le=8192)
    
    # Memory settings
    memory_limit_mb: Optional[int] = Field(default=None, ge=512)
    gc_threshold: float = Field(default=0.8, ge=0.5, le=0.95)
    
    # Async settings
    max_workers: int = Field(default=10, ge=1, le=50)
    queue_size: int = Field(default=1000, ge=100, le=10000)


class ResourceLimits(BaseModel):
    """System resource limits."""

    max_memory_mb: Optional[int] = Field(default=None, ge=512)
    max_cpu_percent: Optional[float] = Field(default=None, ge=10.0, le=100.0)
    max_disk_usage_percent: Optional[float] = Field(default=None, ge=50.0, le=95.0)
    max_connections: Optional[int] = Field(default=None, ge=10, le=10000)
    max_requests_per_second: Optional[float] = Field(default=None, ge=1.0)


class FeatureFlags(BaseModel):
    """Feature flag configuration."""
    
    # Performance features
    enable_query_caching: bool = True
    enable_response_compression: bool = True
    enable_connection_pooling: bool = True
    enable_async_processing: bool = True
    
    # Monitoring features
    enable_metrics_collection: bool = True
    enable_distributed_tracing: bool = True
    enable_performance_monitoring: bool = True
    enable_health_checks: bool = True
    
    # Security features
    enable_rate_limiting: bool = True
    enable_input_validation: bool = True
    enable_sql_injection_protection: bool = True
    enable_xss_protection: bool = True
    
    # ETL features
    enable_incremental_processing: bool = True
    enable_data_quality_checks: bool = True
    enable_automatic_retry: bool = True
    enable_parallel_processing: bool = True


class EnhancedConfigManager:
    """Enhanced configuration manager with advanced features."""
    
    def __init__(self, 
                 config_dir: Path,
                 environment: str = "development",
                 enable_hot_reload: bool = True,
                 enable_secrets: bool = True):
        """
        Initialize enhanced configuration manager.
        
        Args:
            config_dir: Configuration directory path
            environment: Current environment
            enable_hot_reload: Enable configuration hot reloading
            enable_secrets: Enable secrets management integration
        """
        self.config_dir = Path(config_dir)
        self.environment = environment
        self.enable_hot_reload = enable_hot_reload
        self.enable_secrets = enable_secrets
        
        # Configuration state
        self._config_data: Dict[str, Any] = {}
        self._sources: List[ConfigSource] = []
        self._watchers: Dict[str, Observer] = {}
        self._reload_callbacks: List[Callable] = []
        self._lock = threading.RLock()
        
        # Performance and feature settings
        self._performance: Optional[PerformanceSettings] = None
        self._resource_limits: Optional[ResourceLimits] = None
        self._feature_flags: Optional[FeatureFlags] = None
        
        # Initialize secrets manager
        if enable_secrets:
            secret_config = SecretConfig(provider='env')  # Use environment provider for development
            self._secret_manager = SecretManager(secret_config)
        else:
            self._secret_manager = None
        
        # Load initial configuration
        self._load_all_configs()
        
        # Setup hot reload if enabled
        if self.enable_hot_reload:
            self._setup_hot_reload()
    
    def _load_all_configs(self) -> None:
        """Load all configuration files."""
        with self._lock:
            # Clear existing data
            self._config_data.clear()
            self._sources.clear()
            
            # Load configurations in priority order
            self._load_default_config()
            self._load_environment_config()
            self._load_service_configs()
            self._load_environment_variables()
            
            # Initialize specialized settings
            self._initialize_specialized_settings()
    
    def _load_default_config(self) -> None:
        """Load default configuration."""
        default_config_path = self.config_dir / "default.yaml"
        if default_config_path.exists():
            self._load_config_file(
                default_config_path,
                ConfigPriority.DEFAULT,
                ConfigScope.GLOBAL
            )
    
    def _load_environment_config(self) -> None:
        """Load environment-specific configuration."""
        env_config_path = self.config_dir / f"{self.environment}.yaml"
        if env_config_path.exists():
            self._load_config_file(
                env_config_path,
                ConfigPriority.CONFIG_FILE,
                ConfigScope.ENVIRONMENT
            )
    
    def _load_service_configs(self) -> None:
        """Load service-specific configurations."""
        services_dir = self.config_dir / "services"
        if services_dir.exists():
            for config_file in services_dir.glob("*.yaml"):
                self._load_config_file(
                    config_file,
                    ConfigPriority.CONFIG_FILE,
                    ConfigScope.SERVICE
                )
    
    def _load_config_file(self, 
                         path: Path, 
                         priority: ConfigPriority,
                         scope: ConfigScope) -> None:
        """Load a single configuration file."""
        try:
            with open(path, 'r', encoding='utf-8') as f:
                if path.suffix.lower() in ['.yaml', '.yml']:
                    data = yaml.safe_load(f) or {}
                elif path.suffix.lower() == '.json':
                    data = json.load(f)
                else:
                    logger.warning(f"Unsupported config format: {path}")
                    return
            
            # Create source metadata
            source = ConfigSource(
                path=path,
                format=ConfigFormat.YAML if path.suffix.lower() in ['.yaml', '.yml'] else ConfigFormat.JSON,
                priority=priority,
                scope=scope,
                last_modified=datetime.fromtimestamp(path.stat().st_mtime),
                checksum=source.calculate_checksum(data) if 'source' in locals() else None
            )
            
            # Process secrets if enabled
            if self.enable_secrets and self._secret_manager:
                data = self._process_secrets(data)
            
            # Merge configuration data
            self._merge_config_data(data, source)
            self._sources.append(source)
            
            logger.info(f"Loaded configuration from {path}")
            
        except Exception as e:
            logger.error(f"Failed to load config from {path}: {e}")
    
    def _process_secrets(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Process secret references in configuration."""
        def process_value(value):
            if isinstance(value, str) and value.startswith("${SECRET:"):
                secret_key = value[9:-1]  # Remove ${SECRET: and }
                try:
                    return self._secret_manager.get_secret(secret_key)
                except Exception as e:
                    logger.error(f"Failed to resolve secret {secret_key}: {e}")
                    return value
            elif isinstance(value, dict):
                return {k: process_value(v) for k, v in value.items()}
            elif isinstance(value, list):
                return [process_value(v) for v in value]
            return value
        
        return process_value(data)
    
    def _merge_config_data(self, data: Dict[str, Any], source: ConfigSource) -> None:
        """Merge configuration data with priority handling."""
        def deep_merge(base: Dict, overlay: Dict) -> Dict:
            result = base.copy()
            for key, value in overlay.items():
                if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                    result[key] = deep_merge(result[key], value)
                else:
                    result[key] = value
            return result
        
        self._config_data = deep_merge(self._config_data, data)
    
    def _load_environment_variables(self) -> None:
        """Load configuration from environment variables."""
        env_vars = {}
        prefix = "APP_"
        
        for key, value in os.environ.items():
            if key.startswith(prefix):
                config_key = key[len(prefix):].lower().replace('_', '.')
                
                # Try to parse as JSON for complex values
                try:
                    parsed_value = json.loads(value)
                    env_vars[config_key] = parsed_value
                except json.JSONDecodeError:
                    env_vars[config_key] = value
        
        if env_vars:
            source = ConfigSource(
                priority=ConfigPriority.ENVIRONMENT_VARS,
                scope=ConfigScope.ENVIRONMENT,
                last_modified=datetime.utcnow()
            )
            self._merge_config_data(env_vars, source)
            self._sources.append(source)
    
    def _initialize_specialized_settings(self) -> None:
        """Initialize specialized configuration objects."""
        try:
            # Performance settings
            perf_data = self._config_data.get("performance", {})
            self._performance = PerformanceSettings(**perf_data)
            
            # Resource limits
            limits_data = self._config_data.get("resource_limits", {})
            self._resource_limits = ResourceLimits(**limits_data)
            
            # Feature flags
            features_data = self._config_data.get("features", {})
            self._feature_flags = FeatureFlags(**features_data)
            
            logger.info("Initialized specialized configuration settings")
            
        except Exception as e:
            logger.error(f"Failed to initialize specialized settings: {e}")
            # Use defaults
            self._performance = PerformanceSettings()
            self._resource_limits = ResourceLimits()
            self._feature_flags = FeatureFlags()
    
    def _setup_hot_reload(self) -> None:
        """Setup configuration hot reloading."""
        class ConfigFileHandler(FileSystemEventHandler):
            def __init__(self, manager: EnhancedConfigManager):
                self.manager = manager
            
            def on_modified(self, event):
                if not event.is_directory:
                    path = Path(event.src_path)
                    if path.suffix.lower() in ['.yaml', '.yml', '.json']:
                        logger.info(f"Configuration file changed: {path}")
                        asyncio.create_task(self.manager._reload_config())
        
        handler = ConfigFileHandler(self)
        observer = Observer()
        observer.schedule(handler, str(self.config_dir), recursive=True)
        observer.start()
        
        self._watchers["main"] = observer
        logger.info("Configuration hot reload enabled")
    
    async def _reload_config(self) -> None:
        """Reload configuration asynchronously."""
        try:
            # Reload all configurations
            self._load_all_configs()
            
            # Notify callbacks
            for callback in self._reload_callbacks:
                try:
                    if asyncio.iscoroutinefunction(callback):
                        await callback(self)
                    else:
                        callback(self)
                except Exception as e:
                    logger.error(f"Config reload callback failed: {e}")
            
            logger.info("Configuration reloaded successfully")
            
        except Exception as e:
            logger.error(f"Failed to reload configuration: {e}")
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value by key."""
        keys = key.split('.')
        value = self._config_data
        
        for k in keys:
            if isinstance(value, dict) and k in value:
                value = value[k]
            else:
                return default
        
        return value
    
    def set(self, key: str, value: Any, priority: ConfigPriority = ConfigPriority.RUNTIME_OVERRIDE) -> None:
        """Set configuration value at runtime."""
        with self._lock:
            keys = key.split('.')
            target = self._config_data
            
            # Navigate to the parent of the target key
            for k in keys[:-1]:
                if k not in target:
                    target[k] = {}
                target = target[k]
            
            # Set the value
            target[keys[-1]] = value
            
            # Record the source
            source = ConfigSource(
                priority=priority,
                scope=ConfigScope.APPLICATION,
                last_modified=datetime.utcnow()
            )
            self._sources.append(source)
    
    @property
    def performance(self) -> PerformanceSettings:
        """Get performance settings."""
        return self._performance or PerformanceSettings()
    
    @property
    def resource_limits(self) -> ResourceLimits:
        """Get resource limits."""
        return self._resource_limits or ResourceLimits()
    
    @property
    def feature_flags(self) -> FeatureFlags:
        """Get feature flags."""
        return self._feature_flags or FeatureFlags()
    
    def add_reload_callback(self, callback: Callable) -> None:
        """Add callback for configuration reload events."""
        self._reload_callbacks.append(callback)
    
    def get_sources(self) -> List[ConfigSource]:
        """Get all configuration sources."""
        return self._sources.copy()
    
    async def validate_configuration(self) -> List[str]:
        """Validate current configuration and return any errors."""
        errors = []
        
        try:
            # Validate performance settings
            PerformanceSettings(**self._config_data.get("performance", {}))
        except Exception as e:
            errors.append(f"Performance configuration invalid: {e}")
        
        try:
            # Validate resource limits
            ResourceLimits(**self._config_data.get("resource_limits", {}))
        except Exception as e:
            errors.append(f"Resource limits configuration invalid: {e}")
        
        try:
            # Validate feature flags
            FeatureFlags(**self._config_data.get("features", {}))
        except Exception as e:
            errors.append(f"Feature flags configuration invalid: {e}")
        
        return errors
    
    def export_config(self, format: ConfigFormat = ConfigFormat.YAML) -> str:
        """Export current configuration in specified format."""
        if format == ConfigFormat.YAML:
            return yaml.dump(self._config_data, default_flow_style=False)
        elif format == ConfigFormat.JSON:
            return json.dumps(self._config_data, indent=2)
        else:
            raise ValueError(f"Unsupported export format: {format}")
    
    def cleanup(self) -> None:
        """Cleanup resources."""
        for observer in self._watchers.values():
            observer.stop()
            observer.join()
        self._watchers.clear()


# Global configuration manager instance
_config_manager: Optional[EnhancedConfigManager] = None


def initialize_config(config_dir: Path = Path("config"), 
                     environment: Optional[str] = None) -> EnhancedConfigManager:
    """Initialize global configuration manager."""
    global _config_manager
    
    env = environment or os.getenv("ENVIRONMENT", "development")
    
    _config_manager = EnhancedConfigManager(
        config_dir=config_dir,
        environment=env,
        enable_hot_reload=env in ["development", "testing"],
        enable_secrets=True
    )
    
    logger.info(f"Configuration manager initialized for environment: {env}")
    return _config_manager


def get_config() -> EnhancedConfigManager:
    """Get global configuration manager."""
    if not _config_manager:
        return initialize_config()
    return _config_manager


@asynccontextmanager
async def config_context():
    """Context manager for configuration lifecycle."""
    config = get_config()
    try:
        yield config
    finally:
        config.cleanup()