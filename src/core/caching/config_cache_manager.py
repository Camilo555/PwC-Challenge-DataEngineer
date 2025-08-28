"""
Configuration and Metadata Caching System

Comprehensive caching system for application configurations, metadata,
feature flags, and system settings with hot-reloading capabilities.
"""

import asyncio
import json
import os
import time
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union, Callable
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

import yaml
from pydantic import BaseModel

from core.logging import get_logger
from core.caching.redis_cache_manager import RedisCacheManager, get_cache_manager
from core.caching.cache_patterns import RefreshAheadPattern
from core.caching.redis_streams import publish_cache_invalidation, EventType

logger = get_logger(__name__)


class ConfigType(Enum):
    """Configuration types."""
    APPLICATION = "application"
    DATABASE = "database"
    MODEL = "model"
    FEATURE_FLAGS = "feature_flags"
    SECRETS = "secrets"
    METADATA = "metadata"
    SCHEMA = "schema"
    PIPELINE = "pipeline"
    MONITORING = "monitoring"


class ConfigSource(Enum):
    """Configuration sources."""
    FILE = "file"
    DATABASE = "database"
    ENVIRONMENT = "environment"
    REMOTE_SERVICE = "remote_service"
    REDIS = "redis"


@dataclass
class ConfigEntry:
    """Configuration entry metadata."""
    key: str
    config_type: ConfigType
    source: ConfigSource
    value: Any
    created_at: datetime
    updated_at: datetime
    expires_at: Optional[datetime] = None
    version: str = "1.0"
    tags: List[str] = None
    checksum: Optional[str] = None
    is_sensitive: bool = False
    reload_on_change: bool = True
    
    def __post_init__(self):
        if self.tags is None:
            self.tags = []
    
    def is_expired(self) -> bool:
        """Check if config entry is expired."""
        if self.expires_at is None:
            return False
        return datetime.utcnow() > self.expires_at


class FileWatcherHandler(FileSystemEventHandler):
    """File system event handler for configuration files."""
    
    def __init__(self, config_cache: 'ConfigCacheManager'):
        self.config_cache = config_cache
        self.debounce_delay = 1.0  # 1 second debounce
        self.pending_changes = {}
        
    def on_modified(self, event):
        """Handle file modification events."""
        if event.is_directory:
            return
        
        file_path = event.src_path
        current_time = time.time()
        
        # Debounce rapid file changes
        if file_path in self.pending_changes:
            if current_time - self.pending_changes[file_path] < self.debounce_delay:
                return
        
        self.pending_changes[file_path] = current_time
        
        # Schedule reload
        asyncio.create_task(self.config_cache.reload_file_config(file_path))
        
        logger.info(f"Configuration file modified: {file_path}")


class ConfigCacheManager:
    """Advanced configuration and metadata caching manager."""
    
    def __init__(self, cache_manager: Optional[RedisCacheManager] = None,
                 default_ttl: int = 3600,  # 1 hour
                 enable_file_watching: bool = True):
        self.cache_manager = cache_manager
        self.default_ttl = default_ttl
        self.enable_file_watching = enable_file_watching
        self.namespace = "config_cache"
        self.metadata_namespace = "config_meta"
        
        # Configuration sources
        self.config_loaders: Dict[ConfigSource, Callable] = {}
        self.watched_files: Set[str] = set()
        self.file_observer: Optional[Observer] = None
        
        # Schema validation
        self.config_schemas: Dict[str, BaseModel] = {}
        
        # Hot reload callbacks
        self.reload_callbacks: Dict[str, List[Callable]] = {}
        
        # Statistics
        self.stats = {
            "hits": 0,
            "misses": 0,
            "reloads": 0,
            "validations": 0,
            "errors": 0
        }
        
        # Initialize default loaders
        self._register_default_loaders()
        
        # Start file watching if enabled
        if enable_file_watching:
            self._start_file_watching()
    
    async def _get_cache_manager(self) -> RedisCacheManager:
        """Get cache manager instance."""
        if self.cache_manager is None:
            self.cache_manager = await get_cache_manager()
        return self.cache_manager
    
    def _register_default_loaders(self):
        """Register default configuration loaders."""
        self.config_loaders[ConfigSource.FILE] = self._load_from_file
        self.config_loaders[ConfigSource.ENVIRONMENT] = self._load_from_environment
    
    def _start_file_watching(self):
        """Start file system watching for configuration files."""
        try:
            from watchdog.observers import Observer
            
            self.file_observer = Observer()
            event_handler = FileWatcherHandler(self)
            
            # We'll add specific directories to watch when configs are registered
            logger.info("File watching system initialized")
            
        except ImportError:
            logger.warning("Watchdog not available, file watching disabled")
            self.enable_file_watching = False
    
    def register_config_loader(self, source: ConfigSource, loader: Callable):
        """Register a configuration loader for a specific source."""
        self.config_loaders[source] = loader
        logger.info(f"Registered config loader for source: {source.value}")
    
    def register_config_schema(self, config_key: str, schema: BaseModel):
        """Register Pydantic schema for configuration validation."""
        self.config_schemas[config_key] = schema
        logger.info(f"Registered schema for config: {config_key}")
    
    def register_reload_callback(self, config_key: str, callback: Callable):
        """Register callback to be called when configuration is reloaded."""
        if config_key not in self.reload_callbacks:
            self.reload_callbacks[config_key] = []
        self.reload_callbacks[config_key].append(callback)
        logger.info(f"Registered reload callback for config: {config_key}")
    
    async def _load_from_file(self, file_path: str, **kwargs) -> Any:
        """Load configuration from file."""
        try:
            path = Path(file_path)
            
            if not path.exists():
                raise FileNotFoundError(f"Configuration file not found: {file_path}")
            
            with open(path, 'r', encoding='utf-8') as f:
                if path.suffix.lower() in ['.yml', '.yaml']:
                    return yaml.safe_load(f)
                elif path.suffix.lower() == '.json':
                    return json.load(f)
                else:
                    return f.read()
                    
        except Exception as e:
            logger.error(f"Error loading config from file {file_path}: {e}")
            raise
    
    async def _load_from_environment(self, env_prefix: str, **kwargs) -> Dict[str, str]:
        """Load configuration from environment variables."""
        config = {}
        
        for key, value in os.environ.items():
            if key.startswith(env_prefix):
                config_key = key[len(env_prefix):].lower()
                config[config_key] = value
        
        return config
    
    def _build_config_key(self, key: str, version: str = "latest") -> str:
        """Build cache key for configuration."""
        return f"config:{key}:{version}"
    
    def _calculate_checksum(self, value: Any) -> str:
        """Calculate checksum for configuration value."""
        import hashlib
        
        if isinstance(value, dict):
            serialized = json.dumps(value, sort_keys=True, default=str)
        else:
            serialized = str(value)
        
        return hashlib.md5(serialized.encode()).hexdigest()
    
    def _validate_config(self, key: str, value: Any) -> Any:
        """Validate configuration against registered schema."""
        if key not in self.config_schemas:
            return value
        
        try:
            schema = self.config_schemas[key]
            
            if isinstance(value, dict):
                validated = schema(**value)
                self.stats["validations"] += 1
                return validated.dict()
            else:
                # Try direct validation
                validated = schema(value)
                self.stats["validations"] += 1
                return validated
                
        except Exception as e:
            logger.error(f"Config validation error for {key}: {e}")
            self.stats["errors"] += 1
            raise
    
    async def set_config(self, key: str, value: Any, 
                        config_type: ConfigType = ConfigType.APPLICATION,
                        ttl: Optional[int] = None,
                        version: str = "latest",
                        tags: Optional[List[str]] = None,
                        is_sensitive: bool = False) -> bool:
        """Set configuration value in cache."""
        try:
            cache_manager = await self._get_cache_manager()
            
            # Validate configuration if schema exists
            validated_value = self._validate_config(key, value)
            
            # Build cache key
            cache_key = self._build_config_key(key, version)
            
            # Calculate checksum
            checksum = self._calculate_checksum(validated_value)
            
            # Prepare config entry
            config_entry = ConfigEntry(
                key=key,
                config_type=config_type,
                source=ConfigSource.REDIS,  # Since we're setting it directly
                value=validated_value,
                created_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
                expires_at=datetime.utcnow() + timedelta(seconds=ttl or self.default_ttl),
                version=version,
                tags=tags or [],
                checksum=checksum,
                is_sensitive=is_sensitive
            )
            
            # Store in cache
            cache_ttl = ttl or self.default_ttl
            success = await cache_manager.set(
                cache_key, validated_value, cache_ttl, self.namespace
            )
            
            if success:
                # Store metadata
                await self._store_config_metadata(config_entry)
                
                # Publish configuration change event
                await publish_cache_invalidation(
                    f"config_change:{key}",
                    self.namespace,
                    source="config_cache_manager"
                )
                
                logger.info(f"Set configuration: {key} (version: {version})")
            
            return success
            
        except Exception as e:
            logger.error(f"Error setting configuration {key}: {e}")
            self.stats["errors"] += 1
            return False
    
    async def get_config(self, key: str, version: str = "latest",
                        default: Any = None, 
                        source: Optional[ConfigSource] = None,
                        source_params: Optional[Dict[str, Any]] = None) -> Any:
        """Get configuration value from cache or source."""
        try:
            cache_manager = await self._get_cache_manager()
            
            # Build cache key
            cache_key = self._build_config_key(key, version)
            
            # Try cache first
            cached_value = await cache_manager.get(cache_key, self.namespace)
            
            if cached_value is not None:
                self.stats["hits"] += 1
                logger.debug(f"Config cache hit: {key}")
                return cached_value
            
            self.stats["misses"] += 1
            
            # Load from source if specified
            if source and source in self.config_loaders:
                try:
                    loader = self.config_loaders[source]
                    loaded_value = await loader(key, **(source_params or {}))
                    
                    if loaded_value is not None:
                        # Cache the loaded value
                        await self.set_config(
                            key, loaded_value, 
                            config_type=ConfigType.APPLICATION,
                            version=version
                        )
                        return loaded_value
                        
                except Exception as e:
                    logger.error(f"Error loading config from {source.value}: {e}")
            
            # Return default if nothing found
            logger.debug(f"Config not found: {key}, returning default")
            return default
            
        except Exception as e:
            logger.error(f"Error getting configuration {key}: {e}")
            self.stats["errors"] += 1
            return default
    
    async def load_config_file(self, file_path: str, key_prefix: str = "",
                             config_type: ConfigType = ConfigType.APPLICATION,
                             ttl: Optional[int] = None,
                             auto_reload: bool = True) -> bool:
        """Load configuration from file and cache it."""
        try:
            # Load configuration
            config_data = await self._load_from_file(file_path)
            
            if not isinstance(config_data, dict):
                # Single value config
                config_key = key_prefix or Path(file_path).stem
                return await self.set_config(
                    config_key, config_data, config_type, ttl
                )
            
            # Multiple configs from dict
            success_count = 0
            for key, value in config_data.items():
                config_key = f"{key_prefix}.{key}" if key_prefix else key
                
                if await self.set_config(config_key, value, config_type, ttl):
                    success_count += 1
            
            # Set up file watching if enabled
            if auto_reload and self.enable_file_watching:
                self._add_watched_file(file_path, key_prefix)
            
            logger.info(f"Loaded {success_count} configs from file: {file_path}")
            return success_count > 0
            
        except Exception as e:
            logger.error(f"Error loading config file {file_path}: {e}")
            self.stats["errors"] += 1
            return False
    
    def _add_watched_file(self, file_path: str, key_prefix: str = ""):
        """Add file to watch list for auto-reload."""
        if not self.enable_file_watching or not self.file_observer:
            return
        
        abs_path = os.path.abspath(file_path)
        directory = os.path.dirname(abs_path)
        
        if abs_path not in self.watched_files:
            self.watched_files.add(abs_path)
            
            # Add directory to observer
            if not self.file_observer.is_alive():
                self.file_observer.start()
            
            self.file_observer.schedule(
                FileWatcherHandler(self), 
                directory, 
                recursive=False
            )
            
            logger.info(f"Added file to watch list: {abs_path}")
    
    async def reload_file_config(self, file_path: str) -> bool:
        """Reload configuration from file."""
        try:
            # Find configs that came from this file
            # This is a simplified approach - in production you'd track file->config mappings
            
            config_data = await self._load_from_file(file_path)
            
            if isinstance(config_data, dict):
                for key, value in config_data.items():
                    await self.set_config(key, value)
                    
                    # Call reload callbacks
                    if key in self.reload_callbacks:
                        for callback in self.reload_callbacks[key]:
                            try:
                                if asyncio.iscoroutinefunction(callback):
                                    await callback(key, value)
                                else:
                                    callback(key, value)
                            except Exception as e:
                                logger.error(f"Error in reload callback for {key}: {e}")
            
            self.stats["reloads"] += 1
            logger.info(f"Reloaded configuration from file: {file_path}")
            return True
            
        except Exception as e:
            logger.error(f"Error reloading config file {file_path}: {e}")
            self.stats["errors"] += 1
            return False
    
    async def get_config_metadata(self, key: str, version: str = "latest") -> Optional[ConfigEntry]:
        """Get configuration metadata."""
        cache_manager = await self._get_cache_manager()
        
        metadata_key = f"meta:{self._build_config_key(key, version)}"
        metadata = await cache_manager.get(metadata_key, self.metadata_namespace)
        
        if metadata:
            # Convert back to ConfigEntry
            metadata["created_at"] = datetime.fromisoformat(metadata["created_at"])
            metadata["updated_at"] = datetime.fromisoformat(metadata["updated_at"])
            
            if metadata.get("expires_at"):
                metadata["expires_at"] = datetime.fromisoformat(metadata["expires_at"])
            
            metadata["config_type"] = ConfigType(metadata["config_type"])
            metadata["source"] = ConfigSource(metadata["source"])
            
            return ConfigEntry(**metadata)
        
        return None
    
    async def list_configs(self, config_type: Optional[ConfigType] = None,
                          tags: Optional[List[str]] = None,
                          include_sensitive: bool = False) -> List[Dict[str, Any]]:
        """List cached configurations."""
        cache_manager = await self._get_cache_manager()
        
        try:
            # Get all config metadata keys
            meta_pattern = cache_manager._build_key("meta:config:*", self.metadata_namespace)
            
            if cache_manager.is_cluster:
                all_keys = []
                for node in cache_manager.async_redis_client.get_nodes():
                    keys = await node.keys(meta_pattern)
                    if keys:
                        all_keys.extend(keys)
            else:
                all_keys = await cache_manager.async_redis_client.keys(meta_pattern)
            
            configs = []
            
            for key in all_keys:
                try:
                    metadata = await cache_manager.get(
                        key.replace(cache_manager.key_prefix + self.metadata_namespace + ":", ""),
                        self.metadata_namespace
                    )
                    
                    if metadata:
                        # Filter by type
                        if config_type and metadata.get("config_type") != config_type.value:
                            continue
                        
                        # Filter by tags
                        if tags:
                            config_tags = metadata.get("tags", [])
                            if not any(tag in config_tags for tag in tags):
                                continue
                        
                        # Filter sensitive configs
                        if not include_sensitive and metadata.get("is_sensitive", False):
                            continue
                        
                        configs.append({
                            "key": metadata["key"],
                            "config_type": metadata["config_type"],
                            "version": metadata["version"],
                            "created_at": metadata["created_at"],
                            "updated_at": metadata["updated_at"],
                            "is_sensitive": metadata.get("is_sensitive", False),
                            "tags": metadata.get("tags", [])
                        })
                
                except Exception as e:
                    logger.error(f"Error processing config metadata key {key}: {e}")
            
            return configs
            
        except Exception as e:
            logger.error(f"Error listing configurations: {e}")
            return []
    
    async def invalidate_config(self, key: str, version: str = "latest") -> bool:
        """Invalidate cached configuration."""
        cache_manager = await self._get_cache_manager()
        
        cache_key = self._build_config_key(key, version)
        
        # Delete config and metadata
        data_deleted = await cache_manager.delete(cache_key, self.namespace)
        await cache_manager.delete(f"meta:{cache_key}", self.metadata_namespace)
        
        # Publish invalidation event
        await publish_cache_invalidation(
            f"config_invalidate:{key}",
            self.namespace,
            source="config_cache_manager"
        )
        
        logger.info(f"Invalidated configuration: {key} (version: {version})")
        return data_deleted
    
    async def invalidate_configs_by_type(self, config_type: ConfigType) -> int:
        """Invalidate all configurations of a specific type."""
        configs = await self.list_configs(config_type=config_type, include_sensitive=True)
        
        invalidated_count = 0
        for config in configs:
            if await self.invalidate_config(config["key"], config["version"]):
                invalidated_count += 1
        
        logger.info(f"Invalidated {invalidated_count} configs of type: {config_type.value}")
        return invalidated_count
    
    async def _store_config_metadata(self, config_entry: ConfigEntry):
        """Store configuration metadata."""
        cache_manager = await self._get_cache_manager()
        
        metadata_key = f"meta:{self._build_config_key(config_entry.key, config_entry.version)}"
        
        # Convert to dict for storage (excluding sensitive value)
        entry_dict = asdict(config_entry)
        entry_dict.pop("value", None)  # Don't store value in metadata
        entry_dict["created_at"] = entry_dict["created_at"].isoformat()
        entry_dict["updated_at"] = entry_dict["updated_at"].isoformat()
        
        if entry_dict.get("expires_at"):
            entry_dict["expires_at"] = entry_dict["expires_at"].isoformat()
        
        entry_dict["config_type"] = entry_dict["config_type"].value
        entry_dict["source"] = entry_dict["source"].value
        
        # Calculate TTL for metadata (longer than config data)
        if config_entry.expires_at:
            ttl = int((config_entry.expires_at - datetime.utcnow()).total_seconds()) + 300
        else:
            ttl = self.default_ttl + 300
        
        await cache_manager.set(
            metadata_key, entry_dict, ttl, self.metadata_namespace
        )
    
    async def get_cache_statistics(self) -> Dict[str, Any]:
        """Get comprehensive configuration cache statistics."""
        cache_manager = await self._get_cache_manager()
        
        stats = {
            "basic_stats": self.stats.copy(),
            "config_types": {},
            "file_watching": {
                "enabled": self.enable_file_watching,
                "watched_files": len(self.watched_files),
                "observer_active": self.file_observer.is_alive() if self.file_observer else False
            },
            "schemas": {
                "registered": len(self.config_schemas),
                "schemas": list(self.config_schemas.keys())
            },
            "callbacks": {
                "registered": sum(len(callbacks) for callbacks in self.reload_callbacks.values()),
                "configs_with_callbacks": len(self.reload_callbacks)
            }
        }
        
        try:
            # Get config type distribution
            configs = await self.list_configs(include_sensitive=True)
            
            for config in configs:
                config_type = config["config_type"]
                if config_type not in stats["config_types"]:
                    stats["config_types"][config_type] = {"count": 0, "sensitive": 0}
                
                stats["config_types"][config_type]["count"] += 1
                
                if config.get("is_sensitive", False):
                    stats["config_types"][config_type]["sensitive"] += 1
            
            stats["total_configs"] = len(configs)
            
        except Exception as e:
            logger.error(f"Error getting config cache statistics: {e}")
            stats["error"] = str(e)
        
        return stats
    
    def shutdown(self):
        """Shutdown the configuration cache manager."""
        if self.file_observer and self.file_observer.is_alive():
            self.file_observer.stop()
            self.file_observer.join()
        
        logger.info("Configuration cache manager shutdown")


# Global configuration cache manager instance
_config_cache_manager: Optional[ConfigCacheManager] = None


async def get_config_cache_manager() -> ConfigCacheManager:
    """Get or create global configuration cache manager instance."""
    global _config_cache_manager
    if _config_cache_manager is None:
        _config_cache_manager = ConfigCacheManager()
    return _config_cache_manager


# Convenience functions and decorators

async def get_config(key: str, default: Any = None, **kwargs) -> Any:
    """Convenience function to get configuration."""
    config_manager = await get_config_cache_manager()
    return await config_manager.get_config(key, default=default, **kwargs)


async def set_config(key: str, value: Any, **kwargs) -> bool:
    """Convenience function to set configuration."""
    config_manager = await get_config_cache_manager()
    return await config_manager.set_config(key, value, **kwargs)


def config_property(key: str, default: Any = None, config_type: ConfigType = ConfigType.APPLICATION):
    """Decorator to create a configuration property."""
    def decorator(cls):
        def getter(self):
            # This would be implemented to work with your class structure
            # For now, it's a placeholder
            return default
        
        def setter(self, value):
            # Async setter would need special handling
            pass
        
        setattr(cls, key.replace(".", "_"), property(getter, setter))
        return cls
    
    return decorator


class ConfigManager:
    """High-level configuration manager with caching."""
    
    def __init__(self):
        self._cache_manager = None
        self._configs = {}
    
    async def _get_cache_manager(self):
        if self._cache_manager is None:
            self._cache_manager = await get_config_cache_manager()
        return self._cache_manager
    
    async def load_from_file(self, file_path: str, **kwargs):
        """Load configuration from file."""
        cache_manager = await self._get_cache_manager()
        return await cache_manager.load_config_file(file_path, **kwargs)
    
    async def get(self, key: str, default: Any = None, **kwargs):
        """Get configuration value."""
        cache_manager = await self._get_cache_manager()
        return await cache_manager.get_config(key, default=default, **kwargs)
    
    async def set(self, key: str, value: Any, **kwargs):
        """Set configuration value."""
        cache_manager = await self._get_cache_manager()
        return await cache_manager.set_config(key, value, **kwargs)
    
    async def reload_all(self):
        """Reload all configurations."""
        cache_manager = await self._get_cache_manager()
        self._configs.clear()
        
        # This would trigger reloading of all watched files
        # Implementation depends on your specific requirements