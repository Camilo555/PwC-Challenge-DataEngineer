"""
Centralized Configuration Management System
Enterprise-grade configuration management with environment-aware settings,
secret management, validation, hot reloading, and comprehensive audit trails.
"""
from __future__ import annotations

import hashlib
import json
import os
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import yaml
from pydantic import BaseModel, Field, validator
from watchdog.events import FileSystemEventHandler
from watchdog.observers import Observer

from core.logging import get_logger

logger = get_logger(__name__)


class ConfigChangeType:
    """Configuration change types for audit logging."""
    CREATE = "create"
    UPDATE = "update"
    DELETE = "delete"
    RELOAD = "reload"
    VALIDATE = "validate"
    ENCRYPT = "encrypt"
    DECRYPT = "decrypt"


@dataclass
class ConfigChange:
    """Configuration change record for audit trail."""
    timestamp: datetime
    change_type: str
    key: str
    old_value: Optional[Any] = None
    new_value: Optional[Any] = None
    user: Optional[str] = None
    source: str = "system"
    validation_status: str = "unknown"
    metadata: Dict[str, Any] = field(default_factory=dict)


class ConfigValidationRule(BaseModel):
    """Configuration validation rule definition."""
    key_pattern: str = Field(..., description="Regex pattern for configuration keys")
    value_type: str = Field(..., description="Expected value type: str, int, float, bool, list, dict")
    required: bool = Field(False, description="Whether this configuration is required")
    min_value: Optional[Union[int, float]] = Field(None, description="Minimum value for numeric types")
    max_value: Optional[Union[int, float]] = Field(None, description="Maximum value for numeric types")
    allowed_values: Optional[List[Any]] = Field(None, description="List of allowed values")
    validation_function: Optional[str] = Field(None, description="Custom validation function name")
    description: str = Field("", description="Human-readable description of the configuration")

    @validator('value_type')
    def validate_value_type(cls, v):
        """Validate that value_type is one of the supported types."""
        allowed_types = {'str', 'int', 'float', 'bool', 'list', 'dict', 'any'}
        if v not in allowed_types:
            raise ValueError(f"value_type must be one of {allowed_types}")
        return v


class SecretManager:
    """Secure secret management with encryption and access control."""

    def __init__(self, encryption_key: Optional[str] = None):
        """Initialize secret manager with optional encryption key."""
        self.encryption_key = encryption_key or os.environ.get('CONFIG_ENCRYPTION_KEY')
        self._secrets: Dict[str, str] = {}
        self._access_log: List[Dict[str, Any]] = []

    def set_secret(self, key: str, value: str, user: str = "system") -> bool:
        """Store an encrypted secret."""
        try:
            encrypted_value = self._encrypt(value) if self.encryption_key else value
            self._secrets[key] = encrypted_value

            self._access_log.append({
                'timestamp': datetime.utcnow().isoformat(),
                'action': 'set_secret',
                'key': key,
                'user': user,
                'success': True
            })

            logger.info(f"Secret '{key}' stored successfully", extra={'user': user})
            return True

        except Exception as e:
            logger.error(f"Failed to store secret '{key}': {e}")
            self._access_log.append({
                'timestamp': datetime.utcnow().isoformat(),
                'action': 'set_secret',
                'key': key,
                'user': user,
                'success': False,
                'error': str(e)
            })
            return False

    def get_secret(self, key: str, user: str = "system") -> Optional[str]:
        """Retrieve and decrypt a secret."""
        try:
            encrypted_value = self._secrets.get(key)
            if encrypted_value is None:
                return None

            decrypted_value = self._decrypt(encrypted_value) if self.encryption_key else encrypted_value

            self._access_log.append({
                'timestamp': datetime.utcnow().isoformat(),
                'action': 'get_secret',
                'key': key,
                'user': user,
                'success': True
            })

            return decrypted_value

        except Exception as e:
            logger.error(f"Failed to retrieve secret '{key}': {e}")
            self._access_log.append({
                'timestamp': datetime.utcnow().isoformat(),
                'action': 'get_secret',
                'key': key,
                'user': user,
                'success': False,
                'error': str(e)
            })
            return None

    def _encrypt(self, value: str) -> str:
        """Encrypt a value using the encryption key."""
        # Simple XOR encryption for demonstration
        # In production, use proper encryption like Fernet
        if not self.encryption_key:
            return value

        key_bytes = self.encryption_key.encode('utf-8')
        value_bytes = value.encode('utf-8')

        encrypted = bytearray()
        for i, byte in enumerate(value_bytes):
            encrypted.append(byte ^ key_bytes[i % len(key_bytes)])

        return encrypted.hex()

    def _decrypt(self, encrypted_value: str) -> str:
        """Decrypt a value using the encryption key."""
        if not self.encryption_key:
            return encrypted_value

        try:
            encrypted_bytes = bytearray.fromhex(encrypted_value)
            key_bytes = self.encryption_key.encode('utf-8')

            decrypted = bytearray()
            for i, byte in enumerate(encrypted_bytes):
                decrypted.append(byte ^ key_bytes[i % len(key_bytes)])

            return decrypted.decode('utf-8')
        except Exception:
            # If decryption fails, return the original value
            return encrypted_value

    def list_secrets(self) -> List[str]:
        """List all secret keys (not values)."""
        return list(self._secrets.keys())

    def get_access_log(self) -> List[Dict[str, Any]]:
        """Get secret access audit log."""
        return self._access_log.copy()


class ConfigFileWatcher(FileSystemEventHandler):
    """File system watcher for configuration hot reloading."""

    def __init__(self, config_manager: 'CentralizedConfigManager'):
        """Initialize file watcher with reference to config manager."""
        self.config_manager = config_manager
        self.last_reload = {}

    def on_modified(self, event):
        """Handle file modification events."""
        if event.is_directory:
            return

        file_path = event.src_path
        if not any(file_path.endswith(ext) for ext in ['.yaml', '.yml', '.json', '.env']):
            return

        # Debounce rapid file changes
        current_time = time.time()
        if file_path in self.last_reload:
            if current_time - self.last_reload[file_path] < 1.0:  # 1 second debounce
                return

        self.last_reload[file_path] = current_time

        logger.info(f"Configuration file changed: {file_path}")
        self.config_manager.reload_configuration(file_path)


class CentralizedConfigManager:
    """
    Enterprise centralized configuration management system with:
    - Multi-environment support
    - Secret management with encryption
    - Configuration validation and schema enforcement
    - Hot reloading with file watching
    - Comprehensive audit trails
    - Environment variable interpolation
    - Configuration inheritance and overrides
    """

    def __init__(
        self,
        config_directories: Optional[List[str]] = None,
        environment: str = "development",
        enable_hot_reload: bool = True,
        enable_validation: bool = True,
        encryption_key: Optional[str] = None
    ):
        """Initialize the centralized configuration manager."""
        self.environment = environment
        self.enable_hot_reload = enable_hot_reload
        self.enable_validation = enable_validation

        # Configuration storage
        self._config: Dict[str, Any] = {}
        self._config_metadata: Dict[str, Dict[str, Any]] = {}
        self._config_lock = threading.RLock()

        # Validation rules
        self._validation_rules: Dict[str, ConfigValidationRule] = {}

        # Change tracking
        self._change_history: List[ConfigChange] = []
        self._last_reload: Dict[str, float] = {}

        # Secret management
        self.secret_manager = SecretManager(encryption_key)

        # File watching
        self._file_observer: Optional[Observer] = None
        self._watched_files: Set[str] = set()

        # Configuration directories
        self.config_directories = config_directories or [
            "./config",
            "./configs",
            str(Path.home() / ".config" / "pwc-data-platform"),
            "/etc/pwc-data-platform"
        ]

        # Initialize configuration
        self._load_default_configurations()
        self._setup_file_watching()

    def _load_default_configurations(self):
        """Load default configurations from various sources."""
        # Load from config directories
        for config_dir in self.config_directories:
            if os.path.exists(config_dir):
                self._load_from_directory(config_dir)

        # Load environment-specific configurations
        self._load_environment_config()

        # Load from environment variables
        self._load_from_environment()

        logger.info(f"Loaded configuration for environment: {self.environment}")

    def _load_from_directory(self, directory: str):
        """Load configuration files from a directory."""
        config_path = Path(directory)
        if not config_path.exists():
            return

        # Load base configuration files
        for pattern in ["*.yaml", "*.yml", "*.json"]:
            for file_path in config_path.glob(pattern):
                self._load_config_file(str(file_path))

        # Load environment-specific files
        env_patterns = [
            f"*{self.environment}*.yaml",
            f"*{self.environment}*.yml",
            f"*{self.environment}*.json"
        ]

        for pattern in env_patterns:
            for file_path in config_path.glob(pattern):
                self._load_config_file(str(file_path))

    def _load_config_file(self, file_path: str):
        """Load configuration from a specific file."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                if file_path.endswith('.json'):
                    config_data = json.load(f)
                else:  # YAML
                    config_data = yaml.safe_load(f)

            if config_data:
                self._merge_configuration(config_data, source=file_path)
                self._watched_files.add(file_path)

            logger.debug(f"Loaded configuration from: {file_path}")

        except Exception as e:
            logger.error(f"Failed to load configuration from {file_path}: {e}")

    def _load_environment_config(self):
        """Load environment-specific configuration."""
        env_config_file = f"config.{self.environment}.yaml"

        for config_dir in self.config_directories:
            env_file_path = os.path.join(config_dir, env_config_file)
            if os.path.exists(env_file_path):
                self._load_config_file(env_file_path)

    def _load_from_environment(self):
        """Load configuration from environment variables."""
        env_config = {}

        # Load all environment variables with specific prefixes
        prefixes = ['PWC_', 'CONFIG_', 'APP_']

        for key, value in os.environ.items():
            for prefix in prefixes:
                if key.startswith(prefix):
                    # Convert ENV_VAR_NAME to nested dict structure
                    config_key = key[len(prefix):].lower().replace('_', '.')
                    self._set_nested_value(env_config, config_key, self._parse_env_value(value))

        if env_config:
            self._merge_configuration(env_config, source="environment_variables")

    def _parse_env_value(self, value: str) -> Any:
        """Parse environment variable value to appropriate type."""
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

    def _set_nested_value(self, config_dict: Dict[str, Any], key_path: str, value: Any):
        """Set a nested configuration value using dot notation."""
        keys = key_path.split('.')
        current = config_dict

        for key in keys[:-1]:
            if key not in current:
                current[key] = {}
            current = current[key]

        current[keys[-1]] = value

    def _merge_configuration(self, new_config: Dict[str, Any], source: str = "unknown"):
        """Merge new configuration with existing configuration."""
        with self._config_lock:
            old_config = self._config.copy()
            self._deep_merge(self._config, new_config)

            # Record change
            change = ConfigChange(
                timestamp=datetime.utcnow(),
                change_type=ConfigChangeType.UPDATE,
                key="__root__",
                old_value=old_config,
                new_value=self._config.copy(),
                source=source
            )
            self._change_history.append(change)

            # Validate if enabled
            if self.enable_validation:
                self._validate_configuration()

    def _deep_merge(self, target: Dict[str, Any], source: Dict[str, Any]):
        """Recursively merge source dict into target dict."""
        for key, value in source.items():
            if key in target and isinstance(target[key], dict) and isinstance(value, dict):
                self._deep_merge(target[key], value)
            else:
                target[key] = value

    def _setup_file_watching(self):
        """Setup file system watching for hot reloading."""
        if not self.enable_hot_reload:
            return

        try:
            self._file_observer = Observer()
            event_handler = ConfigFileWatcher(self)

            # Watch configuration directories
            for config_dir in self.config_directories:
                if os.path.exists(config_dir):
                    self._file_observer.schedule(event_handler, config_dir, recursive=True)

            self._file_observer.start()
            logger.info("Configuration file watching enabled")

        except Exception as e:
            logger.error(f"Failed to setup file watching: {e}")

    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value by key with dot notation support."""
        with self._config_lock:
            keys = key.split('.')
            current = self._config

            try:
                for k in keys:
                    current = current[k]

                # Handle secret references
                if isinstance(current, str) and current.startswith('${SECRET:') and current.endswith('}'):
                    secret_key = current[9:-1]  # Remove ${SECRET: and }
                    secret_value = self.secret_manager.get_secret(secret_key)
                    return secret_value if secret_value is not None else default

                return current

            except (KeyError, TypeError):
                return default

    def set(self, key: str, value: Any, user: str = "system", validate: bool = True) -> bool:
        """Set configuration value with validation and audit logging."""
        with self._config_lock:
            old_value = self.get(key)

            # Set the value
            keys = key.split('.')
            current = self._config

            for k in keys[:-1]:
                if k not in current:
                    current[k] = {}
                current = current[k]

            current[keys[-1]] = value

            # Record change
            change = ConfigChange(
                timestamp=datetime.utcnow(),
                change_type=ConfigChangeType.UPDATE if old_value is not None else ConfigChangeType.CREATE,
                key=key,
                old_value=old_value,
                new_value=value,
                user=user,
                source="api"
            )

            # Validate if enabled
            if validate and self.enable_validation:
                validation_result = self._validate_single_key(key, value)
                change.validation_status = "passed" if validation_result else "failed"

                if not validation_result:
                    # Revert change if validation fails
                    if old_value is not None:
                        current[keys[-1]] = old_value
                    else:
                        del current[keys[-1]]

                    self._change_history.append(change)
                    logger.warning(f"Configuration change rejected for key '{key}': validation failed")
                    return False
            else:
                change.validation_status = "skipped"

            self._change_history.append(change)
            logger.info(f"Configuration updated: {key} = {value}", extra={'user': user})
            return True

    def delete(self, key: str, user: str = "system") -> bool:
        """Delete configuration value with audit logging."""
        with self._config_lock:
            old_value = self.get(key)
            if old_value is None:
                return False

            keys = key.split('.')
            current = self._config

            try:
                for k in keys[:-1]:
                    current = current[k]

                del current[keys[-1]]

                # Record change
                change = ConfigChange(
                    timestamp=datetime.utcnow(),
                    change_type=ConfigChangeType.DELETE,
                    key=key,
                    old_value=old_value,
                    new_value=None,
                    user=user,
                    source="api"
                )
                self._change_history.append(change)

                logger.info(f"Configuration deleted: {key}", extra={'user': user})
                return True

            except (KeyError, TypeError):
                return False

    def add_validation_rule(self, rule: ConfigValidationRule):
        """Add a configuration validation rule."""
        self._validation_rules[rule.key_pattern] = rule
        logger.info(f"Added validation rule for pattern: {rule.key_pattern}")

    def _validate_configuration(self) -> bool:
        """Validate entire configuration against rules."""
        if not self._validation_rules:
            return True

        validation_errors = []

        def validate_recursive(config_dict: Dict[str, Any], prefix: str = ""):
            for key, value in config_dict.items():
                full_key = f"{prefix}.{key}" if prefix else key

                if isinstance(value, dict):
                    validate_recursive(value, full_key)
                else:
                    if not self._validate_single_key(full_key, value):
                        validation_errors.append(full_key)

        validate_recursive(self._config)

        if validation_errors:
            logger.error(f"Configuration validation failed for keys: {validation_errors}")
            return False

        return True

    def _validate_single_key(self, key: str, value: Any) -> bool:
        """Validate a single configuration key-value pair."""
        import re

        for pattern, rule in self._validation_rules.items():
            if re.match(pattern, key):
                # Type validation
                if rule.value_type != 'any':
                    expected_type = {
                        'str': str,
                        'int': int,
                        'float': float,
                        'bool': bool,
                        'list': list,
                        'dict': dict
                    }.get(rule.value_type)

                    if expected_type and not isinstance(value, expected_type):
                        return False

                # Range validation for numeric types
                if rule.value_type in ['int', 'float'] and isinstance(value, (int, float)):
                    if rule.min_value is not None and value < rule.min_value:
                        return False
                    if rule.max_value is not None and value > rule.max_value:
                        return False

                # Allowed values validation
                if rule.allowed_values and value not in rule.allowed_values:
                    return False

                return True

        return True  # No matching rules means validation passes

    def reload_configuration(self, file_path: Optional[str] = None):
        """Reload configuration from files with hot reload support."""
        try:
            if file_path:
                # Reload specific file
                if file_path in self._watched_files:
                    self._load_config_file(file_path)
            else:
                # Reload all configurations
                with self._config_lock:
                    self._config.clear()
                    self._load_default_configurations()

            # Record reload event
            change = ConfigChange(
                timestamp=datetime.utcnow(),
                change_type=ConfigChangeType.RELOAD,
                key=file_path or "__all__",
                source="file_watcher" if file_path else "manual"
            )
            self._change_history.append(change)

            logger.info(f"Configuration reloaded: {file_path or 'all files'}")

        except Exception as e:
            logger.error(f"Failed to reload configuration: {e}")

    def get_configuration_info(self) -> Dict[str, Any]:
        """Get comprehensive configuration system information."""
        with self._config_lock:
            return {
                'environment': self.environment,
                'total_configuration_keys': len(self._flatten_config(self._config)),
                'validation_rules': len(self._validation_rules),
                'change_history_entries': len(self._change_history),
                'watched_files': list(self._watched_files),
                'hot_reload_enabled': self.enable_hot_reload,
                'validation_enabled': self.enable_validation,
                'secret_keys': self.secret_manager.list_secrets(),
                'config_directories': self.config_directories,
                'last_reload': max(self._last_reload.values()) if self._last_reload else None
            }

    def _flatten_config(self, config_dict: Dict[str, Any], prefix: str = "") -> Dict[str, Any]:
        """Flatten nested configuration dictionary."""
        result = {}

        for key, value in config_dict.items():
            full_key = f"{prefix}.{key}" if prefix else key

            if isinstance(value, dict):
                result.update(self._flatten_config(value, full_key))
            else:
                result[full_key] = value

        return result

    def get_change_history(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Get configuration change audit trail."""
        changes = self._change_history.copy()

        if limit:
            changes = changes[-limit:]

        return [
            {
                'timestamp': change.timestamp.isoformat(),
                'change_type': change.change_type,
                'key': change.key,
                'user': change.user,
                'source': change.source,
                'validation_status': change.validation_status,
                'has_old_value': change.old_value is not None,
                'has_new_value': change.new_value is not None,
                'metadata': change.metadata
            }
            for change in changes
        ]

    def export_configuration(self, include_secrets: bool = False) -> Dict[str, Any]:
        """Export current configuration for backup or migration."""
        with self._config_lock:
            config_export = {
                'environment': self.environment,
                'timestamp': datetime.utcnow().isoformat(),
                'configuration': self._config.copy(),
                'validation_rules': {
                    pattern: rule.dict() for pattern, rule in self._validation_rules.items()
                }
            }

            if include_secrets:
                config_export['secrets'] = {
                    key: self.secret_manager.get_secret(key)
                    for key in self.secret_manager.list_secrets()
                }

            return config_export

    def import_configuration(self, config_data: Dict[str, Any], user: str = "system") -> bool:
        """Import configuration from exported data."""
        try:
            if 'configuration' in config_data:
                self._merge_configuration(config_data['configuration'], source=f"import_by_{user}")

            if 'validation_rules' in config_data:
                for pattern, rule_data in config_data['validation_rules'].items():
                    rule = ConfigValidationRule(**rule_data)
                    self.add_validation_rule(rule)

            if 'secrets' in config_data:
                for key, value in config_data['secrets'].items():
                    self.secret_manager.set_secret(key, value, user)

            logger.info(f"Configuration imported successfully by {user}")
            return True

        except Exception as e:
            logger.error(f"Failed to import configuration: {e}")
            return False

    def shutdown(self):
        """Shutdown the configuration manager and cleanup resources."""
        if self._file_observer:
            self._file_observer.stop()
            self._file_observer.join()

        logger.info("Configuration manager shutdown complete")


# Global configuration manager instance
_config_manager: Optional[CentralizedConfigManager] = None


def get_config_manager(**kwargs) -> CentralizedConfigManager:
    """Get or create the global configuration manager."""
    global _config_manager

    if _config_manager is None:
        _config_manager = CentralizedConfigManager(**kwargs)

    return _config_manager


def get_config(key: str, default: Any = None) -> Any:
    """Convenience function to get configuration value."""
    return get_config_manager().get(key, default)


def set_config(key: str, value: Any, user: str = "system") -> bool:
    """Convenience function to set configuration value."""
    return get_config_manager().set(key, value, user)


# Export key classes and functions
__all__ = [
    'CentralizedConfigManager',
    'ConfigValidationRule',
    'SecretManager',
    'ConfigChange',
    'ConfigChangeType',
    'get_config_manager',
    'get_config',
    'set_config'
]