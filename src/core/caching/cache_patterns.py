"""
Enterprise Caching Patterns Implementation

Implements cache-aside, write-through, write-behind, and other advanced
caching patterns with Redis backend.
"""

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from collections import defaultdict, deque
from datetime import datetime, timedelta
from functools import wraps
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

from core.logging import get_logger
from core.caching.redis_cache_manager import RedisCacheManager, get_cache_manager

logger = get_logger(__name__)


class CachePattern(ABC):
    """Abstract base class for cache patterns."""
    
    def __init__(self, cache_manager: Optional[RedisCacheManager] = None, 
                 namespace: str = "pattern"):
        self.cache_manager = cache_manager
        self.namespace = namespace
        
    async def _get_cache_manager(self) -> RedisCacheManager:
        """Get cache manager instance."""
        if self.cache_manager is None:
            self.cache_manager = await get_cache_manager()
        return self.cache_manager
    
    @abstractmethod
    async def get(self, key: str, load_func: Optional[Callable] = None, **kwargs) -> Any:
        """Get value using specific pattern."""
        pass
    
    @abstractmethod
    async def set(self, key: str, value: Any, **kwargs) -> bool:
        """Set value using specific pattern."""
        pass


class CacheAsidePattern(CachePattern):
    """
    Cache-Aside (Lazy Loading) Pattern
    
    Application is responsible for loading data into cache on cache miss.
    """
    
    def __init__(self, cache_manager: Optional[RedisCacheManager] = None, 
                 default_ttl: int = 3600):
        super().__init__(cache_manager, "cache_aside")
        self.default_ttl = default_ttl
        
    async def get(self, key: str, load_func: Optional[Callable] = None, 
                 ttl: Optional[int] = None, **kwargs) -> Any:
        """
        Get value with cache-aside pattern.
        
        Args:
            key: Cache key
            load_func: Function to load data on cache miss
            ttl: Time to live for cached data
            **kwargs: Additional arguments passed to load_func
        """
        cache_manager = await self._get_cache_manager()
        
        # Try to get from cache first
        value = await cache_manager.get(key, self.namespace)
        if value is not None:
            logger.debug(f"Cache hit for key: {key}")
            return value
        
        logger.debug(f"Cache miss for key: {key}")
        
        # If load_func provided, call it and cache the result
        if load_func:
            try:
                if asyncio.iscoroutinefunction(load_func):
                    value = await load_func(**kwargs)
                else:
                    value = load_func(**kwargs)
                
                # Cache the loaded value
                cache_ttl = ttl or self.default_ttl
                await cache_manager.set(key, value, cache_ttl, self.namespace)
                
                logger.debug(f"Loaded and cached value for key: {key}")
                return value
                
            except Exception as e:
                logger.error(f"Failed to load data for key {key}: {e}")
                return None
        
        return None
    
    async def set(self, key: str, value: Any, ttl: Optional[int] = None, **kwargs) -> bool:
        """Set value in cache only."""
        cache_manager = await self._get_cache_manager()
        cache_ttl = ttl or self.default_ttl
        return await cache_manager.set(key, value, cache_ttl, self.namespace)
    
    async def invalidate(self, key: str) -> bool:
        """Remove value from cache."""
        cache_manager = await self._get_cache_manager()
        return await cache_manager.delete(key, self.namespace)


class WriteThroughPattern(CachePattern):
    """
    Write-Through Pattern
    
    Writes go to cache and data store simultaneously.
    """
    
    def __init__(self, cache_manager: Optional[RedisCacheManager] = None,
                 write_func: Optional[Callable] = None,
                 read_func: Optional[Callable] = None,
                 default_ttl: int = 3600):
        super().__init__(cache_manager, "write_through")
        self.write_func = write_func
        self.read_func = read_func
        self.default_ttl = default_ttl
        
    async def get(self, key: str, load_func: Optional[Callable] = None, 
                 ttl: Optional[int] = None, **kwargs) -> Any:
        """Get value, loading from data store if not in cache."""
        cache_manager = await self._get_cache_manager()
        
        # Try cache first
        value = await cache_manager.get(key, self.namespace)
        if value is not None:
            return value
        
        # Use provided load_func or default read_func
        loader = load_func or self.read_func
        if loader:
            try:
                if asyncio.iscoroutinefunction(loader):
                    value = await loader(key, **kwargs)
                else:
                    value = loader(key, **kwargs)
                
                if value is not None:
                    cache_ttl = ttl or self.default_ttl
                    await cache_manager.set(key, value, cache_ttl, self.namespace)
                
                return value
                
            except Exception as e:
                logger.error(f"Failed to load data for key {key}: {e}")
        
        return None
    
    async def set(self, key: str, value: Any, ttl: Optional[int] = None, 
                 write_func: Optional[Callable] = None, **kwargs) -> bool:
        """Set value in both cache and data store."""
        cache_manager = await self._get_cache_manager()
        
        # Write to data store first
        writer = write_func or self.write_func
        if writer:
            try:
                if asyncio.iscoroutinefunction(writer):
                    write_success = await writer(key, value, **kwargs)
                else:
                    write_success = writer(key, value, **kwargs)
                
                if not write_success:
                    logger.error(f"Failed to write to data store for key: {key}")
                    return False
                    
            except Exception as e:
                logger.error(f"Data store write error for key {key}: {e}")
                return False
        
        # Write to cache
        cache_ttl = ttl or self.default_ttl
        cache_success = await cache_manager.set(key, value, cache_ttl, self.namespace)
        
        return cache_success
    
    async def delete(self, key: str, delete_func: Optional[Callable] = None, **kwargs) -> bool:
        """Delete from both cache and data store."""
        cache_manager = await self._get_cache_manager()
        
        # Delete from data store
        deleter = delete_func
        if deleter:
            try:
                if asyncio.iscoroutinefunction(deleter):
                    await deleter(key, **kwargs)
                else:
                    deleter(key, **kwargs)
                    
            except Exception as e:
                logger.error(f"Data store delete error for key {key}: {e}")
        
        # Delete from cache
        return await cache_manager.delete(key, self.namespace)


class WriteBehindPattern(CachePattern):
    """
    Write-Behind (Write-Back) Pattern
    
    Writes go to cache immediately, then asynchronously to data store.
    """
    
    def __init__(self, cache_manager: Optional[RedisCacheManager] = None,
                 write_func: Optional[Callable] = None,
                 batch_size: int = 100,
                 flush_interval: int = 60,
                 max_retries: int = 3):
        super().__init__(cache_manager, "write_behind")
        self.write_func = write_func
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self.max_retries = max_retries
        
        # Write buffer
        self.write_buffer: Dict[str, Any] = {}
        self.write_times: Dict[str, datetime] = {}
        self.retry_counts: Dict[str, int] = defaultdict(int)
        
        # Start background flush task
        self._flush_task = None
        self._running = False
        
    async def start_background_flush(self):
        """Start background flush task."""
        if not self._running:
            self._running = True
            self._flush_task = asyncio.create_task(self._background_flush())
            logger.info("Write-behind background flush started")
    
    async def stop_background_flush(self):
        """Stop background flush task."""
        self._running = False
        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
        logger.info("Write-behind background flush stopped")
    
    async def _background_flush(self):
        """Background task to flush write buffer."""
        while self._running:
            try:
                await asyncio.sleep(self.flush_interval)
                await self.flush_buffer()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Background flush error: {e}")
    
    async def get(self, key: str, load_func: Optional[Callable] = None, **kwargs) -> Any:
        """Get value from cache or load from data store."""
        cache_manager = await self._get_cache_manager()
        
        # Check write buffer first
        if key in self.write_buffer:
            return self.write_buffer[key]
        
        # Try cache
        value = await cache_manager.get(key, self.namespace)
        if value is not None:
            return value
        
        # Load from data store
        if load_func:
            try:
                if asyncio.iscoroutinefunction(load_func):
                    value = await load_func(key, **kwargs)
                else:
                    value = load_func(key, **kwargs)
                
                if value is not None:
                    await cache_manager.set(key, value, namespace=self.namespace)
                
                return value
                
            except Exception as e:
                logger.error(f"Failed to load data for key {key}: {e}")
        
        return None
    
    async def set(self, key: str, value: Any, **kwargs) -> bool:
        """Set value in cache and buffer for async write."""
        cache_manager = await self._get_cache_manager()
        
        # Write to cache immediately
        cache_success = await cache_manager.set(key, value, namespace=self.namespace)
        
        if cache_success:
            # Add to write buffer
            self.write_buffer[key] = value
            self.write_times[key] = datetime.utcnow()
            
            # Flush if buffer is full
            if len(self.write_buffer) >= self.batch_size:
                await self.flush_buffer()
        
        return cache_success
    
    async def flush_buffer(self, force_all: bool = False) -> int:
        """Flush write buffer to data store."""
        if not self.write_buffer or not self.write_func:
            return 0
        
        flushed_count = 0
        keys_to_remove = []
        
        current_time = datetime.utcnow()
        
        for key, value in list(self.write_buffer.items()):
            # Check if should flush (forced, buffer full, or time passed)
            should_flush = (
                force_all or 
                len(self.write_buffer) >= self.batch_size or
                (current_time - self.write_times[key]).seconds >= self.flush_interval
            )
            
            if should_flush:
                try:
                    if asyncio.iscoroutinefunction(self.write_func):
                        success = await self.write_func(key, value)
                    else:
                        success = self.write_func(key, value)
                    
                    if success:
                        keys_to_remove.append(key)
                        flushed_count += 1
                        self.retry_counts[key] = 0
                    else:
                        self.retry_counts[key] += 1
                        if self.retry_counts[key] >= self.max_retries:
                            logger.error(f"Max retries exceeded for key: {key}")
                            keys_to_remove.append(key)
                
                except Exception as e:
                    logger.error(f"Write error for key {key}: {e}")
                    self.retry_counts[key] += 1
                    if self.retry_counts[key] >= self.max_retries:
                        keys_to_remove.append(key)
        
        # Remove successfully written or failed keys
        for key in keys_to_remove:
            self.write_buffer.pop(key, None)
            self.write_times.pop(key, None)
            self.retry_counts.pop(key, None)
        
        if flushed_count > 0:
            logger.debug(f"Flushed {flushed_count} items from write buffer")
        
        return flushed_count


class ReadThroughPattern(CachePattern):
    """
    Read-Through Pattern
    
    Cache sits between application and data store, loading data transparently.
    """
    
    def __init__(self, cache_manager: Optional[RedisCacheManager] = None,
                 load_func: Callable = None,
                 default_ttl: int = 3600):
        super().__init__(cache_manager, "read_through")
        self.load_func = load_func
        self.default_ttl = default_ttl
        
    async def get(self, key: str, load_func: Optional[Callable] = None, 
                 ttl: Optional[int] = None, **kwargs) -> Any:
        """Get value, automatically loading from data store if not cached."""
        cache_manager = await self._get_cache_manager()
        
        # Try cache first
        value = await cache_manager.get(key, self.namespace)
        if value is not None:
            return value
        
        # Load from data store
        loader = load_func or self.load_func
        if loader:
            try:
                if asyncio.iscoroutinefunction(loader):
                    value = await loader(key, **kwargs)
                else:
                    value = loader(key, **kwargs)
                
                if value is not None:
                    cache_ttl = ttl or self.default_ttl
                    await cache_manager.set(key, value, cache_ttl, self.namespace)
                
                return value
                
            except Exception as e:
                logger.error(f"Failed to load data for key {key}: {e}")
        
        return None
    
    async def set(self, key: str, value: Any, ttl: Optional[int] = None, **kwargs) -> bool:
        """Set value in cache (read-through doesn't handle writes)."""
        cache_manager = await self._get_cache_manager()
        cache_ttl = ttl or self.default_ttl
        return await cache_manager.set(key, value, cache_ttl, self.namespace)


class RefreshAheadPattern(CachePattern):
    """
    Refresh-Ahead Pattern
    
    Proactively refreshes cache entries before they expire.
    """
    
    def __init__(self, cache_manager: Optional[RedisCacheManager] = None,
                 load_func: Callable = None,
                 default_ttl: int = 3600,
                 refresh_threshold: float = 0.8):
        super().__init__(cache_manager, "refresh_ahead")
        self.load_func = load_func
        self.default_ttl = default_ttl
        self.refresh_threshold = refresh_threshold  # Refresh when TTL < threshold * original_ttl
        
        # Track refresh tasks
        self.refresh_tasks: Dict[str, asyncio.Task] = {}
        
    async def get(self, key: str, load_func: Optional[Callable] = None, 
                 ttl: Optional[int] = None, **kwargs) -> Any:
        """Get value with proactive refresh."""
        cache_manager = await self._get_cache_manager()
        
        value = await cache_manager.get(key, self.namespace)
        
        if value is not None:
            # Check if should refresh
            remaining_ttl = await cache_manager.ttl(key, self.namespace)
            original_ttl = ttl or self.default_ttl
            
            if remaining_ttl > 0 and remaining_ttl < (original_ttl * self.refresh_threshold):
                # Trigger background refresh if not already running
                if key not in self.refresh_tasks or self.refresh_tasks[key].done():
                    self.refresh_tasks[key] = asyncio.create_task(
                        self._refresh_key(key, load_func, original_ttl, **kwargs)
                    )
            
            return value
        
        # Cache miss - load immediately
        loader = load_func or self.load_func
        if loader:
            try:
                if asyncio.iscoroutinefunction(loader):
                    value = await loader(key, **kwargs)
                else:
                    value = loader(key, **kwargs)
                
                if value is not None:
                    cache_ttl = ttl or self.default_ttl
                    await cache_manager.set(key, value, cache_ttl, self.namespace)
                
                return value
                
            except Exception as e:
                logger.error(f"Failed to load data for key {key}: {e}")
        
        return None
    
    async def _refresh_key(self, key: str, load_func: Optional[Callable], 
                          ttl: int, **kwargs):
        """Background refresh of cache key."""
        try:
            loader = load_func or self.load_func
            if loader:
                if asyncio.iscoroutinefunction(loader):
                    value = await loader(key, **kwargs)
                else:
                    value = loader(key, **kwargs)
                
                if value is not None:
                    cache_manager = await self._get_cache_manager()
                    await cache_manager.set(key, value, ttl, self.namespace)
                    logger.debug(f"Refreshed cache key: {key}")
                    
        except Exception as e:
            logger.error(f"Failed to refresh key {key}: {e}")
    
    async def set(self, key: str, value: Any, ttl: Optional[int] = None, **kwargs) -> bool:
        """Set value in cache."""
        cache_manager = await self._get_cache_manager()
        cache_ttl = ttl or self.default_ttl
        return await cache_manager.set(key, value, cache_ttl, self.namespace)


class CacheDecoratorFactory:
    """Factory for creating cache decorators with different patterns."""
    
    @staticmethod
    def cached(pattern: str = "cache_aside", ttl: int = 3600, 
               key_builder: Optional[Callable] = None,
               namespace: str = "decorated", **pattern_kwargs):
        """
        Create a caching decorator.
        
        Args:
            pattern: Caching pattern to use
            ttl: Time to live for cached data
            key_builder: Function to build cache key from arguments
            namespace: Cache namespace
            **pattern_kwargs: Additional arguments for the pattern
        """
        def decorator(func):
            @wraps(func)
            async def wrapper(*args, **kwargs):
                # Get or create cache pattern
                cache_manager = await get_cache_manager()
                
                if pattern == "cache_aside":
                    cache_pattern = CacheAsidePattern(cache_manager, ttl)
                elif pattern == "write_through":
                    cache_pattern = WriteThroughPattern(cache_manager, **pattern_kwargs)
                elif pattern == "read_through":
                    cache_pattern = ReadThroughPattern(cache_manager, func, ttl)
                elif pattern == "refresh_ahead":
                    cache_pattern = RefreshAheadPattern(cache_manager, func, ttl, **pattern_kwargs)
                else:
                    raise ValueError(f"Unknown cache pattern: {pattern}")
                
                # Build cache key
                if key_builder:
                    cache_key = key_builder(*args, **kwargs)
                else:
                    # Default key building
                    key_parts = [func.__name__]
                    key_parts.extend(str(arg) for arg in args)
                    key_parts.extend(f"{k}:{v}" for k, v in sorted(kwargs.items()))
                    cache_key = ":".join(key_parts)
                
                cache_pattern.namespace = namespace
                
                # Try to get from cache
                if pattern in ["cache_aside", "read_through", "refresh_ahead"]:
                    return await cache_pattern.get(cache_key, func, ttl=ttl, **kwargs)
                else:
                    # For write patterns, execute function and cache result
                    if asyncio.iscoroutinefunction(func):
                        result = await func(*args, **kwargs)
                    else:
                        result = func(*args, **kwargs)
                    
                    await cache_pattern.set(cache_key, result, ttl=ttl)
                    return result
            
            return wrapper
        return decorator
    
    @staticmethod
    def cache_invalidate(pattern: str, key_builder: Optional[Callable] = None,
                        namespace: str = "decorated"):
        """
        Create a cache invalidation decorator.
        
        Args:
            pattern: Cache key pattern to invalidate
            key_builder: Function to build cache key from arguments
            namespace: Cache namespace
        """
        def decorator(func):
            @wraps(func)
            async def wrapper(*args, **kwargs):
                # Execute function first
                if asyncio.iscoroutinefunction(func):
                    result = await func(*args, **kwargs)
                else:
                    result = func(*args, **kwargs)
                
                # Invalidate cache
                cache_manager = await get_cache_manager()
                
                if key_builder:
                    cache_key = key_builder(*args, **kwargs)
                else:
                    cache_key = pattern
                
                if "*" in cache_key:
                    # Pattern invalidation
                    await cache_manager.invalidate_pattern(cache_key, namespace)
                else:
                    # Single key invalidation
                    await cache_manager.delete(cache_key, namespace)
                
                return result
            
            return wrapper
        return decorator


# Convenience functions for common patterns

async def cache_aside_get(key: str, load_func: Callable, ttl: int = 3600, **kwargs) -> Any:
    """Convenience function for cache-aside pattern."""
    pattern = CacheAsidePattern(default_ttl=ttl)
    return await pattern.get(key, load_func, ttl=ttl, **kwargs)


async def cache_aside_set(key: str, value: Any, ttl: int = 3600) -> bool:
    """Convenience function for cache-aside set."""
    pattern = CacheAsidePattern(default_ttl=ttl)
    return await pattern.set(key, value, ttl=ttl)


async def write_through_set(key: str, value: Any, write_func: Callable, 
                           ttl: int = 3600, **kwargs) -> bool:
    """Convenience function for write-through pattern."""
    pattern = WriteThroughPattern(write_func=write_func, default_ttl=ttl)
    return await pattern.set(key, value, ttl=ttl, write_func=write_func, **kwargs)