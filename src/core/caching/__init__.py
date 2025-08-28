"""
Enterprise Redis Caching Integration Layer

Comprehensive caching system integration that brings together all caching
components into a unified, easy-to-use interface for the entire data platform.
"""

import asyncio
import logging
from typing import Any, Dict, List, Optional, Union, Callable

from core.logging import get_logger

# Core Redis components
from .redis_cache_manager import RedisCacheManager, get_cache_manager
from .cache_patterns import (
    CacheAsidePattern, WriteThroughPattern, WriteBehindPattern, 
    ReadThroughPattern, RefreshAheadPattern, CacheDecoratorFactory
)
from .redis_streams import (
    RedisStreamsManager, get_streams_manager, EventType, StreamEvent,
    publish_cache_invalidation, publish_cache_warming, publish_model_update
)

# Specialized caching systems
from .session_cache import (
    SessionManager, UserDataCache, RateLimiter,
    get_session_manager, get_user_data_cache, get_rate_limiter
)
from .config_cache_manager import (
    ConfigCacheManager, get_config_cache_manager, 
    get_config, set_config
)

# ML and API caching
from ..ml.caching.ml_cache_manager import MLCacheManager, get_ml_cache_manager
from ..ml.caching.feature_store_cache import FeatureStoreCache, get_feature_store_cache
from ..api.caching.api_cache_manager import APICacheManager, get_api_cache_manager
from ..data_access.caching.query_cache_manager import QueryCacheManager, get_query_cache_manager

# Advanced caching features
from .cache_warming_strategies import (
    CacheWarmingOrchestrator, CacheInvalidationManager,
    get_warming_orchestrator, get_invalidation_manager,
    warm_cache_key, invalidate_by_trigger
)
from .redis_monitoring import RedisMonitor, get_redis_monitor

logger = get_logger(__name__)


class EnterpriseCacheManager:
    """
    Enterprise-wide cache manager that orchestrates all caching components.
    
    This is the main entry point for all caching operations across the platform.
    It provides a unified interface while maintaining the flexibility of 
    specialized caching systems.
    """
    
    def __init__(self):
        # Core managers (will be initialized on first use)
        self._redis_manager: Optional[RedisCacheManager] = None
        self._streams_manager: Optional[RedisStreamsManager] = None
        self._session_manager: Optional[SessionManager] = None
        self._config_manager: Optional[ConfigCacheManager] = None
        self._ml_cache_manager: Optional[MLCacheManager] = None
        self._feature_cache: Optional[FeatureStoreCache] = None
        self._api_cache_manager: Optional[APICacheManager] = None
        self._query_cache_manager: Optional[QueryCacheManager] = None
        self._warming_orchestrator: Optional[CacheWarmingOrchestrator] = None
        self._invalidation_manager: Optional[CacheInvalidationManager] = None
        self._redis_monitor: Optional[RedisMonitor] = None
        
        # Initialization status
        self._initialized = False
        self._initializing = False
        
        logger.info("Enterprise Cache Manager created")
    
    async def initialize(self) -> bool:
        """Initialize all cache managers and start monitoring."""
        if self._initialized:
            return True
            
        if self._initializing:
            # Wait for initialization to complete
            while self._initializing and not self._initialized:
                await asyncio.sleep(0.1)
            return self._initialized
        
        self._initializing = True
        
        try:
            logger.info("Initializing Enterprise Cache Manager...")
            
            # Initialize core Redis manager first
            self._redis_manager = await get_cache_manager()
            
            # Initialize streams for event processing
            self._streams_manager = await get_streams_manager()
            await self._streams_manager.start_consumers()
            
            # Initialize specialized managers
            self._session_manager = await get_session_manager()
            self._config_manager = await get_config_cache_manager()
            self._ml_cache_manager = await get_ml_cache_manager()
            self._feature_cache = await get_feature_store_cache()
            self._api_cache_manager = await get_api_cache_manager()
            self._query_cache_manager = await get_query_cache_manager()
            
            # Initialize warming and invalidation
            self._warming_orchestrator = await get_warming_orchestrator()
            self._invalidation_manager = await get_invalidation_manager()
            
            # Start monitoring
            self._redis_monitor = await get_redis_monitor()
            
            self._initialized = True
            logger.info("Enterprise Cache Manager initialized successfully")
            
            return True
            
        except Exception as e:
            logger.error(f"Failed to initialize Enterprise Cache Manager: {e}")
            return False
        
        finally:
            self._initializing = False
    
    async def shutdown(self):
        """Shutdown all cache managers gracefully."""
        logger.info("Shutting down Enterprise Cache Manager...")
        
        try:
            # Stop streams
            if self._streams_manager:
                await self._streams_manager.stop_consumers()
            
            # Stop warming orchestrator
            if self._warming_orchestrator:
                await self._warming_orchestrator.stop_orchestrator()
            
            # Stop monitoring
            if self._redis_monitor:
                await self._redis_monitor.stop_monitoring()
            
            # Close Redis connections
            if self._redis_manager:
                await self._redis_manager.close()
            
            # Shutdown config manager file watching
            if self._config_manager:
                self._config_manager.shutdown()
            
            self._initialized = False
            logger.info("Enterprise Cache Manager shutdown completed")
            
        except Exception as e:
            logger.error(f"Error during shutdown: {e}")
    
    async def health_check(self) -> Dict[str, Any]:
        """Comprehensive health check of all caching systems."""
        if not self._initialized:
            await self.initialize()
        
        health_status = {
            "overall_status": "healthy",
            "components": {},
            "timestamp": None,
            "issues": []
        }
        
        try:
            # Check Redis core
            if self._redis_manager:
                redis_health = await self._redis_manager.health_check()
                health_status["components"]["redis"] = redis_health
                if redis_health["status"] != "healthy":
                    health_status["overall_status"] = "degraded"
                    health_status["issues"].append("Redis core issues detected")
            
            # Check streams
            if self._streams_manager:
                streams_stats = await self._streams_manager.get_statistics()
                health_status["components"]["streams"] = {
                    "status": "healthy" if streams_stats.get("running", False) else "unhealthy",
                    "active_consumers": streams_stats.get("active_consumers", 0),
                    "processed_events": sum(
                        proc["processed_count"] for proc in streams_stats.get("processors", {}).values()
                    )
                }
            
            # Check specialized caches
            cache_components = {
                "ml_cache": self._ml_cache_manager,
                "feature_cache": self._feature_cache,
                "api_cache": self._api_cache_manager,
                "query_cache": self._query_cache_manager
            }
            
            for name, manager in cache_components.items():
                if manager:
                    try:
                        stats = await manager.get_cache_statistics()
                        health_status["components"][name] = {
                            "status": "healthy",
                            "hit_rate": stats.get("basic_stats", {}).get("hit_rate", 0),
                            "cache_size": stats.get("basic_stats", {}).get("cache_size", 0)
                        }
                    except Exception as e:
                        health_status["components"][name] = {
                            "status": "unhealthy",
                            "error": str(e)
                        }
                        health_status["overall_status"] = "degraded"
                        health_status["issues"].append(f"{name} health check failed")
            
            # Check monitoring
            if self._redis_monitor:
                monitor_stats = await self._redis_monitor.get_monitoring_statistics()
                health_status["components"]["monitoring"] = {
                    "status": "healthy" if monitor_stats.get("is_monitoring", False) else "unhealthy",
                    "uptime": monitor_stats.get("monitoring_stats", {}).get("monitoring_uptime", 0),
                    "alerts_active": len(await self._redis_monitor.get_active_alerts())
                }
            
            health_status["timestamp"] = await self._get_current_timestamp()
            
        except Exception as e:
            health_status["overall_status"] = "unhealthy"
            health_status["issues"].append(f"Health check error: {str(e)}")
            logger.error(f"Health check error: {e}")
        
        return health_status
    
    async def get_comprehensive_statistics(self) -> Dict[str, Any]:
        """Get comprehensive statistics from all caching systems."""
        if not self._initialized:
            await self.initialize()
        
        stats = {
            "overview": {
                "total_cache_hits": 0,
                "total_cache_misses": 0,
                "overall_hit_rate": 0.0,
                "total_cache_size": 0
            },
            "components": {}
        }
        
        try:
            # Collect stats from all managers
            managers = {
                "redis_core": self._redis_manager,
                "ml_cache": self._ml_cache_manager,
                "feature_cache": self._feature_cache,
                "api_cache": self._api_cache_manager,
                "query_cache": self._query_cache_manager,
                "warming_orchestrator": self._warming_orchestrator,
                "invalidation_manager": self._invalidation_manager
            }
            
            total_hits = 0
            total_misses = 0
            total_size = 0
            
            for name, manager in managers.items():
                if manager and hasattr(manager, 'get_cache_statistics'):
                    try:
                        component_stats = await manager.get_cache_statistics()
                        stats["components"][name] = component_stats
                        
                        # Aggregate key metrics
                        basic_stats = component_stats.get("basic_stats", {})
                        total_hits += basic_stats.get("hits", 0)
                        total_misses += basic_stats.get("misses", 0)
                        total_size += basic_stats.get("cache_size", 0)
                        
                    except Exception as e:
                        stats["components"][name] = {"error": str(e)}
            
            # Calculate overall metrics
            total_requests = total_hits + total_misses
            stats["overview"]["total_cache_hits"] = total_hits
            stats["overview"]["total_cache_misses"] = total_misses
            stats["overview"]["overall_hit_rate"] = (
                total_hits / total_requests if total_requests > 0 else 0.0
            )
            stats["overview"]["total_cache_size"] = total_size
            
            # Add Redis monitoring stats
            if self._redis_monitor:
                monitor_stats = await self._redis_monitor.get_monitoring_statistics()
                stats["components"]["monitoring"] = monitor_stats
            
            # Add streams stats
            if self._streams_manager:
                streams_stats = await self._streams_manager.get_statistics()
                stats["components"]["streams"] = streams_stats
            
        except Exception as e:
            logger.error(f"Error collecting comprehensive statistics: {e}")
            stats["error"] = str(e)
        
        return stats
    
    async def clear_all_caches(self, confirm: bool = False) -> Dict[str, Any]:
        """Clear all caches across all systems (use with caution)."""
        if not confirm:
            return {
                "error": "Clear all caches requires confirmation parameter",
                "warning": "This will clear ALL cached data across the entire platform"
            }
        
        if not self._initialized:
            await self.initialize()
        
        results = {}
        
        try:
            logger.warning("Clearing ALL caches across the platform")
            
            # Clear core Redis cache
            if self._redis_manager:
                success = await self._redis_manager.flush_db()
                results["redis_core"] = {"cleared": success}
            
            # Publish cache invalidation event
            await publish_cache_invalidation(
                "*", "all", pattern=True, 
                source="enterprise_cache_manager"
            )
            
            results["status"] = "completed"
            results["timestamp"] = await self._get_current_timestamp()
            
            logger.warning("All caches cleared")
            
        except Exception as e:
            logger.error(f"Error clearing caches: {e}")
            results["error"] = str(e)
        
        return results
    
    async def warm_critical_caches(self) -> Dict[str, Any]:
        """Warm critical caches across all systems."""
        if not self._initialized:
            await self.initialize()
        
        results = {"tasks_scheduled": 0, "errors": []}
        
        try:
            # This would be customized based on your specific critical data
            critical_warming_tasks = [
                # Configuration cache warming
                {
                    "cache_key": "app_config:production",
                    "loader": "load_application_config",
                    "priority": "critical"
                },
                # Feature store warming for popular features
                {
                    "cache_key": "features:popular:*",
                    "loader": "load_popular_features",
                    "priority": "high"
                },
                # ML model metadata warming
                {
                    "cache_key": "ml_models:active:*",
                    "loader": "load_active_models",
                    "priority": "high"
                }
            ]
            
            logger.info(f"Scheduling {len(critical_warming_tasks)} critical cache warming tasks")
            
            results["tasks_scheduled"] = len(critical_warming_tasks)
            results["status"] = "scheduled"
            
        except Exception as e:
            logger.error(f"Error warming critical caches: {e}")
            results["errors"].append(str(e))
        
        return results
    
    async def _get_current_timestamp(self) -> str:
        """Get current timestamp in ISO format."""
        from datetime import datetime
        return datetime.utcnow().isoformat()
    
    # Convenience methods for common operations
    
    async def get(self, key: str, namespace: str = "default", **kwargs) -> Any:
        """Get value from cache (uses core Redis manager)."""
        if not self._initialized:
            await self.initialize()
        return await self._redis_manager.get(key, namespace)
    
    async def set(self, key: str, value: Any, ttl: int = None, 
                 namespace: str = "default", **kwargs) -> bool:
        """Set value in cache (uses core Redis manager)."""
        if not self._initialized:
            await self.initialize()
        return await self._redis_manager.set(key, value, ttl, namespace)
    
    async def delete(self, key: str, namespace: str = "default", **kwargs) -> bool:
        """Delete key from cache (uses core Redis manager)."""
        if not self._initialized:
            await self.initialize()
        return await self._redis_manager.delete(key, namespace)
    
    async def invalidate_pattern(self, pattern: str, namespace: str = "default", **kwargs) -> int:
        """Invalidate keys matching pattern (uses core Redis manager)."""
        if not self._initialized:
            await self.initialize()
        return await self._redis_manager.invalidate_pattern(pattern, namespace)
    
    # Specialized cache access methods
    
    @property
    async def ml_cache(self) -> MLCacheManager:
        """Get ML cache manager."""
        if not self._initialized:
            await self.initialize()
        return self._ml_cache_manager
    
    @property  
    async def feature_cache(self) -> FeatureStoreCache:
        """Get feature store cache."""
        if not self._initialized:
            await self.initialize()
        return self._feature_cache
    
    @property
    async def api_cache(self) -> APICacheManager:
        """Get API cache manager."""
        if not self._initialized:
            await self.initialize()
        return self._api_cache_manager
    
    @property
    async def query_cache(self) -> QueryCacheManager:
        """Get query cache manager."""
        if not self._initialized:
            await self.initialize()
        return self._query_cache_manager
    
    @property
    async def session_manager(self) -> SessionManager:
        """Get session manager."""
        if not self._initialized:
            await self.initialize()
        return self._session_manager
    
    @property
    async def config_cache(self) -> ConfigCacheManager:
        """Get configuration cache manager."""
        if not self._initialized:
            await self.initialize()
        return self._config_manager


# Global enterprise cache manager instance
_enterprise_cache: Optional[EnterpriseCacheManager] = None


async def get_enterprise_cache() -> EnterpriseCacheManager:
    """Get or create global enterprise cache manager instance."""
    global _enterprise_cache
    if _enterprise_cache is None:
        _enterprise_cache = EnterpriseCacheManager()
        await _enterprise_cache.initialize()
    return _enterprise_cache


# Convenience functions for quick access
async def cache_get(key: str, namespace: str = "default", **kwargs) -> Any:
    """Quick cache get operation."""
    cache = await get_enterprise_cache()
    return await cache.get(key, namespace, **kwargs)


async def cache_set(key: str, value: Any, ttl: int = None, 
                   namespace: str = "default", **kwargs) -> bool:
    """Quick cache set operation."""
    cache = await get_enterprise_cache()
    return await cache.set(key, value, ttl, namespace, **kwargs)


async def cache_delete(key: str, namespace: str = "default", **kwargs) -> bool:
    """Quick cache delete operation."""
    cache = await get_enterprise_cache()
    return await cache.delete(key, namespace, **kwargs)


# Export main components
__all__ = [
    # Core managers
    'EnterpriseCacheManager',
    'get_enterprise_cache',
    
    # Convenience functions
    'cache_get',
    'cache_set', 
    'cache_delete',
    
    # Component managers
    'RedisCacheManager',
    'get_cache_manager',
    'RedisStreamsManager',
    'get_streams_manager',
    'SessionManager',
    'get_session_manager',
    'ConfigCacheManager',
    'get_config_cache_manager',
    'MLCacheManager',
    'get_ml_cache_manager',
    'FeatureStoreCache',
    'get_feature_store_cache',
    'APICacheManager',
    'get_api_cache_manager',
    'QueryCacheManager',
    'get_query_cache_manager',
    'CacheWarmingOrchestrator',
    'get_warming_orchestrator',
    'CacheInvalidationManager',
    'get_invalidation_manager',
    'RedisMonitor',
    'get_redis_monitor',
    
    # Cache patterns
    'CacheAsidePattern',
    'WriteThroughPattern',
    'WriteBehindPattern',
    'ReadThroughPattern',
    'RefreshAheadPattern',
    'CacheDecoratorFactory',
    
    # Events and streaming
    'EventType',
    'StreamEvent',
    'publish_cache_invalidation',
    'publish_cache_warming',
    'publish_model_update',
    
    # Utilities
    'get_config',
    'set_config',
    'warm_cache_key',
    'invalidate_by_trigger'
]