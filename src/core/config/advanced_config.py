"""
Advanced Configuration Management System
Provides hierarchical configuration, environment management, and validation.
"""

import os
import json
import yaml
from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional, Union, Type, TypeVar, get_type_hints
from dataclasses import dataclass, field, fields
from pathlib import Path
from enum import Enum
import logging
from datetime import datetime
import hashlib
import threading
from functools import lru_cache

from core.logging import get_logger


T = TypeVar('T')


class ConfigFormat(Enum):
    """Supported configuration formats"""
    JSON = "json"
    YAML = "yaml"
    ENV = "env"
    PYTHON = "python"


class Environment(Enum):
    """Deployment environments"""
    DEVELOPMENT = "development"
    TESTING = "testing"
    STAGING = "staging"
    PRODUCTION = "production"
    LOCAL = "local"


@dataclass
class ConfigMetadata:
    """Metadata about configuration"""
    name: str
    version: str
    description: str
    environment: Environment
    created_at: datetime
    last_modified: datetime
    checksum: str
    source_files: List[str] = field(default_factory=list)
    dependencies: List[str] = field(default_factory=list)


class ConfigValidator(ABC):
    """Abstract base class for configuration validators"""
    
    @abstractmethod
    def validate(self, config: Dict[str, Any]) -> bool:
        """Validate configuration and return True if valid"""
        pass
    
    @abstractmethod
    def get_errors(self) -> List[str]:
        """Get validation errors"""
        pass


class SchemaValidator(ConfigValidator):
    """Validates configuration against a JSON schema"""
    
    def __init__(self, schema: Dict[str, Any]):
        self.schema = schema
        self.errors: List[str] = []
    
    def validate(self, config: Dict[str, Any]) -> bool:
        """Validate configuration against schema"""
        self.errors = []
        
        try:
            import jsonschema
            jsonschema.validate(config, self.schema)
            return True
        except ImportError:
            self.errors.append("jsonschema package not available")
            return False
        except jsonschema.ValidationError as e:
            self.errors.append(f"Schema validation error: {e.message}")
            return False
        except Exception as e:
            self.errors.append(f"Validation error: {str(e)}")
            return False
    
    def get_errors(self) -> List[str]:
        return self.errors


class TypeValidator(ConfigValidator):
    """Validates configuration using type hints"""
    
    def __init__(self, config_class: Type):
        self.config_class = config_class
        self.errors: List[str] = []
        self.type_hints = get_type_hints(config_class)
    
    def validate(self, config: Dict[str, Any]) -> bool:
        """Validate configuration using type hints"""
        self.errors = []
        
        try:
            # Try to instantiate the config class
            self.config_class(**config)
            return True
        except TypeError as e:
            self.errors.append(f"Type validation error: {str(e)}")
            return False
        except Exception as e:
            self.errors.append(f"Validation error: {str(e)}")
            return False
    
    def get_errors(self) -> List[str]:
        return self.errors


class ConfigLoader:
    """Loads configuration from various sources"""
    
    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)
    
    def load_file(self, file_path: Union[str, Path], format: Optional[ConfigFormat] = None) -> Dict[str, Any]:
        """Load configuration from file"""
        file_path = Path(file_path)
        
        if not file_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {file_path}")
        
        # Auto-detect format if not specified
        if format is None:
            format = self._detect_format(file_path)
        
        self.logger.info(f"Loading configuration from {file_path} (format: {format.value})")
        
        with open(file_path, 'r', encoding='utf-8') as f:
            if format == ConfigFormat.JSON:
                return json.load(f)
            elif format == ConfigFormat.YAML:
                return yaml.safe_load(f)
            elif format == ConfigFormat.PYTHON:
                # Execute Python file and extract variables
                return self._load_python_config(file_path)
            else:
                raise ValueError(f"Unsupported format: {format}")
    
    def load_environment(self, prefix: str = "") -> Dict[str, Any]:
        """Load configuration from environment variables"""
        config = {}
        
        for key, value in os.environ.items():
            if not prefix or key.startswith(prefix):
                config_key = key[len(prefix):] if prefix else key
                config[config_key.lower()] = self._convert_env_value(value)
        
        return config
    
    def _detect_format(self, file_path: Path) -> ConfigFormat:
        """Auto-detect configuration format from file extension"""
        suffix = file_path.suffix.lower()
        
        if suffix in ['.json']:
            return ConfigFormat.JSON
        elif suffix in ['.yaml', '.yml']:
            return ConfigFormat.YAML
        elif suffix in ['.py']:
            return ConfigFormat.PYTHON
        else:
            raise ValueError(f"Cannot detect format for file: {file_path}")
    
    def _load_python_config(self, file_path: Path) -> Dict[str, Any]:
        """Load configuration from Python file"""
        import importlib.util
        
        spec = importlib.util.spec_from_file_location("config", file_path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        # Extract variables that don't start with underscore
        config = {}
        for name in dir(module):
            if not name.startswith('_'):
                value = getattr(module, name)
                if not callable(value) and not isinstance(value, type):
                    config[name.lower()] = value
        
        return config
    
    def _convert_env_value(self, value: str) -> Any:
        """Convert environment variable string to appropriate type"""
        # Try to parse as JSON first
        try:
            return json.loads(value)
        except (json.JSONDecodeError, ValueError):
            pass
        
        # Try boolean
        if value.lower() in ('true', 'false'):
            return value.lower() == 'true'
        
        # Try integer
        try:
            return int(value)
        except ValueError:
            pass
        
        # Try float
        try:
            return float(value)
        except ValueError:
            pass
        
        # Return as string
        return value


class ConfigMerger:
    """Merges configurations from multiple sources with priority"""
    
    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)
    
    def merge(self, *configs: Dict[str, Any]) -> Dict[str, Any]:
        """Merge multiple configurations (later configs override earlier ones)"""
        result = {}
        
        for config in configs:
            if config:
                result = self._deep_merge(result, config)
        
        return result
    
    def _deep_merge(self, base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """Deep merge two dictionaries"""
        result = base.copy()
        
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value
        
        return result


class ConfigCache:
    """Caches configuration with invalidation support"""
    
    def __init__(self, max_size: int = 128):
        self.max_size = max_size
        self.cache: Dict[str, Any] = {}
        self.timestamps: Dict[str, datetime] = {}
        self.checksums: Dict[str, str] = {}
        self.lock = threading.Lock()
    
    def get(self, key: str) -> Optional[Any]:
        """Get cached configuration"""
        with self.lock:
            return self.cache.get(key)
    
    def set(self, key: str, value: Any, checksum: Optional[str] = None) -> None:
        """Set cached configuration"""
        with self.lock:
            if len(self.cache) >= self.max_size:
                # Remove oldest entry
                oldest_key = min(self.timestamps.keys(), key=lambda k: self.timestamps[k])
                self._remove(oldest_key)
            
            self.cache[key] = value
            self.timestamps[key] = datetime.now()
            if checksum:
                self.checksums[key] = checksum
    
    def invalidate(self, key: str) -> None:
        """Invalidate cached configuration"""
        with self.lock:
            self._remove(key)
    
    def _remove(self, key: str) -> None:
        """Remove key from cache"""
        self.cache.pop(key, None)
        self.timestamps.pop(key, None)
        self.checksums.pop(key, None)
    
    def is_valid(self, key: str, current_checksum: str) -> bool:
        """Check if cached configuration is still valid"""
        with self.lock:
            return self.checksums.get(key) == current_checksum


class AdvancedConfigManager:
    """Advanced configuration manager with hierarchical loading and validation"""
    
    def __init__(self, 
                 config_dir: Union[str, Path] = "./config",
                 environment: Optional[Environment] = None,
                 cache_enabled: bool = True):
        
        self.config_dir = Path(config_dir)
        self.environment = environment or self._detect_environment()
        self.cache_enabled = cache_enabled
        
        self.loader = ConfigLoader()
        self.merger = ConfigMerger()
        self.cache = ConfigCache() if cache_enabled else None
        self.validators: Dict[str, List[ConfigValidator]] = {}
        
        self.logger = get_logger(self.__class__.__name__)
        
        # Create config directory if it doesn't exist
        self.config_dir.mkdir(parents=True, exist_ok=True)
    
    def _detect_environment(self) -> Environment:
        """Auto-detect current environment"""
        env = os.getenv('ENVIRONMENT', os.getenv('ENV', 'development')).lower()
        
        try:
            return Environment(env)
        except ValueError:
            self.logger.warning(f"Unknown environment '{env}', defaulting to development")
            return Environment.DEVELOPMENT
    
    def register_validator(self, config_name: str, validator: ConfigValidator) -> None:
        """Register a validator for a configuration"""
        if config_name not in self.validators:
            self.validators[config_name] = []
        self.validators[config_name].append(validator)
    
    def load_config(self, 
                   config_name: str,
                   config_class: Optional[Type[T]] = None,
                   validate: bool = True,
                   use_cache: bool = True) -> Union[Dict[str, Any], T]:
        """Load configuration with hierarchical merging"""
        
        cache_key = f"{config_name}_{self.environment.value}"
        
        # Check cache first
        if use_cache and self.cache:
            cached_config = self.cache.get(cache_key)
            if cached_config is not None:
                current_checksum = self._calculate_checksum(config_name)
                if self.cache.is_valid(cache_key, current_checksum):
                    self.logger.debug(f"Using cached configuration for {config_name}")
                    return cached_config
        
        # Load configurations in order of precedence
        configs = []
        source_files = []
        
        # 1. Base configuration
        base_config_path = self.config_dir / f"{config_name}.yaml"
        if base_config_path.exists():
            configs.append(self.loader.load_file(base_config_path))
            source_files.append(str(base_config_path))
        
        # 2. Environment-specific configuration
        env_config_path = self.config_dir / f"{config_name}.{self.environment.value}.yaml"
        if env_config_path.exists():
            configs.append(self.loader.load_file(env_config_path))
            source_files.append(str(env_config_path))
        
        # 3. Local overrides
        local_config_path = self.config_dir / f"{config_name}.local.yaml"
        if local_config_path.exists():
            configs.append(self.loader.load_file(local_config_path))
            source_files.append(str(local_config_path))
        
        # 4. Environment variables
        env_prefix = f"{config_name.upper()}_"
        env_config = self.loader.load_environment(env_prefix)
        if env_config:
            configs.append(env_config)
        
        # Merge all configurations
        merged_config = self.merger.merge(*configs)
        
        if not merged_config:
            raise ValueError(f"No configuration found for '{config_name}'")
        
        # Validate configuration
        if validate:
            self._validate_config(config_name, merged_config)
        
        # Convert to dataclass if requested
        if config_class:
            try:
                result = config_class(**merged_config)
            except TypeError as e:
                raise ValueError(f"Failed to create {config_class.__name__} from configuration: {e}")
        else:
            result = merged_config
        
        # Cache the result
        if use_cache and self.cache:
            checksum = self._calculate_checksum(config_name)
            self.cache.set(cache_key, result, checksum)
        
        self.logger.info(f"Loaded configuration '{config_name}' for environment '{self.environment.value}'")
        
        return result
    
    def _validate_config(self, config_name: str, config: Dict[str, Any]) -> None:
        """Validate configuration using registered validators"""
        if config_name not in self.validators:
            return
        
        all_errors = []
        
        for validator in self.validators[config_name]:
            if not validator.validate(config):
                all_errors.extend(validator.get_errors())
        
        if all_errors:
            error_msg = f"Configuration validation failed for '{config_name}':\n" + "\n".join(f"  - {error}" for error in all_errors)
            raise ValueError(error_msg)
    
    def _calculate_checksum(self, config_name: str) -> str:
        """Calculate checksum for configuration files"""
        hasher = hashlib.md5()
        
        # Include all possible config files
        config_files = [
            self.config_dir / f"{config_name}.yaml",
            self.config_dir / f"{config_name}.{self.environment.value}.yaml",
            self.config_dir / f"{config_name}.local.yaml"
        ]
        
        for config_file in config_files:
            if config_file.exists():
                hasher.update(config_file.read_bytes())
        
        # Include relevant environment variables
        env_prefix = f"{config_name.upper()}_"
        env_vars = sorted([f"{k}={v}" for k, v in os.environ.items() if k.startswith(env_prefix)])
        hasher.update("\n".join(env_vars).encode())
        
        return hasher.hexdigest()
    
    def save_config(self, 
                   config_name: str, 
                   config: Union[Dict[str, Any], Any],
                   environment: Optional[Environment] = None,
                   format: ConfigFormat = ConfigFormat.YAML) -> None:
        """Save configuration to file"""
        
        env = environment or self.environment
        
        if env == Environment.PRODUCTION:
            raise ValueError("Cannot save configuration in production environment")
        
        # Convert dataclass to dict if needed
        if hasattr(config, '__dataclass_fields__'):
            config_dict = {field.name: getattr(config, field.name) for field in fields(config)}
        else:
            config_dict = config
        
        # Determine output file
        if env == Environment.LOCAL:
            output_file = self.config_dir / f"{config_name}.local.yaml"
        else:
            output_file = self.config_dir / f"{config_name}.{env.value}.yaml"
        
        # Save configuration
        with open(output_file, 'w', encoding='utf-8') as f:
            if format == ConfigFormat.YAML:
                yaml.dump(config_dict, f, default_flow_style=False, indent=2)
            elif format == ConfigFormat.JSON:
                json.dump(config_dict, f, indent=2)
            else:
                raise ValueError(f"Unsupported save format: {format}")
        
        # Invalidate cache
        if self.cache:
            cache_key = f"{config_name}_{env.value}"
            self.cache.invalidate(cache_key)
        
        self.logger.info(f"Saved configuration '{config_name}' to {output_file}")
    
    def get_config_metadata(self, config_name: str) -> ConfigMetadata:
        """Get metadata about a configuration"""
        
        source_files = []
        last_modified = datetime.min
        
        # Check all possible config files
        config_files = [
            self.config_dir / f"{config_name}.yaml",
            self.config_dir / f"{config_name}.{self.environment.value}.yaml",
            self.config_dir / f"{config_name}.local.yaml"
        ]
        
        for config_file in config_files:
            if config_file.exists():
                source_files.append(str(config_file))
                file_mtime = datetime.fromtimestamp(config_file.stat().st_mtime)
                last_modified = max(last_modified, file_mtime)
        
        checksum = self._calculate_checksum(config_name)
        
        return ConfigMetadata(
            name=config_name,
            version="1.0.0",  # Could be read from config
            description=f"Configuration for {config_name}",
            environment=self.environment,
            created_at=last_modified,
            last_modified=last_modified,
            checksum=checksum,
            source_files=source_files
        )
    
    def list_configurations(self) -> List[str]:
        """List all available configurations"""
        config_files = list(self.config_dir.glob("*.yaml")) + list(self.config_dir.glob("*.json"))
        
        # Extract base names (without environment suffixes)
        config_names = set()
        for config_file in config_files:
            name = config_file.stem
            # Remove environment suffixes
            for env in Environment:
                if name.endswith(f".{env.value}"):
                    name = name[:-len(f".{env.value}")]
                    break
            if name.endswith(".local"):
                name = name[:-6]  # Remove .local
            
            config_names.add(name)
        
        return sorted(list(config_names))
    
    def reload_config(self, config_name: str) -> None:
        """Force reload configuration (clear cache)"""
        if self.cache:
            cache_key = f"{config_name}_{self.environment.value}"
            self.cache.invalidate(cache_key)
        
        self.logger.info(f"Reloaded configuration: {config_name}")


# Global configuration manager instance
_global_config_manager: Optional[AdvancedConfigManager] = None


def get_config_manager(
    config_dir: Union[str, Path] = "./config",
    environment: Optional[Environment] = None
) -> AdvancedConfigManager:
    """Get the global configuration manager instance"""
    global _global_config_manager
    
    if _global_config_manager is None:
        _global_config_manager = AdvancedConfigManager(config_dir, environment)
    
    return _global_config_manager


@lru_cache(maxsize=32)
def load_config(
    config_name: str,
    config_class: Optional[Type[T]] = None,
    validate: bool = True
) -> Union[Dict[str, Any], T]:
    """Convenience function to load configuration"""
    manager = get_config_manager()
    return manager.load_config(config_name, config_class, validate)