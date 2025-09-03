"""
Advanced Caching Patterns Implementation

Provides enterprise-grade caching patterns including:
- Cache-Aside Pattern
- Write-Through Pattern
- Write-Behind Pattern
- Cache warming strategies
- TTL-based expiration
"""

from __future__ import annotations

import asyncio
import time
from abc import ABC, abstractmethod
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Generic, TypeVar

T = TypeVar("T")


class CachePattern(str, Enum):
    """Cache pattern types."""

    CACHE_ASIDE = "cache_aside"
    WRITE_THROUGH = "write_through"
    WRITE_BEHIND = "write_behind"


@dataclass
class CacheEntry(Generic[T]):
    """Cache entry with metadata."""

    value: T
    created_at: float = field(default_factory=time.time)
    ttl: float | None = None
    access_count: int = 0
    last_accessed: float = field(default_factory=time.time)

    def is_expired(self) -> bool:
        """Check if cache entry is expired."""
        if self.ttl is None:
            return False
        return time.time() - self.created_at > self.ttl

    def access(self) -> None:
        """Record cache access."""
        self.access_count += 1
        self.last_accessed = time.time()


class BaseCachePattern(ABC, Generic[T]):
    """Base class for cache patterns."""

    def __init__(self, default_ttl: float | None = None):
        self.default_ttl = default_ttl
        self._cache: dict[str, CacheEntry[T]] = {}
        self._stats = {"hits": 0, "misses": 0, "evictions": 0, "expired": 0}

    @abstractmethod
    async def get(
        self, key: str, loader: Callable[[], Awaitable[T]], ttl: float | None = None
    ) -> T:
        """Get value from cache or load if not present."""
        pass

    @abstractmethod
    async def put(self, key: str, value: T, ttl: float | None = None) -> None:
        """Put value in cache."""
        pass

    async def delete(self, key: str) -> bool:
        """Delete key from cache."""
        if key in self._cache:
            del self._cache[key]
            return True
        return False

    async def clear(self) -> int:
        """Clear all cache entries."""
        count = len(self._cache)
        self._cache.clear()
        return count

    def _cleanup_expired(self) -> None:
        """Remove expired entries."""
        time.time()
        expired_keys = []

        for key, entry in self._cache.items():
            if entry.is_expired():
                expired_keys.append(key)

        for key in expired_keys:
            del self._cache[key]
            self._stats["expired"] += 1

    def get_stats(self) -> dict[str, Any]:
        """Get cache statistics."""
        total_requests = self._stats["hits"] + self._stats["misses"]
        hit_rate = self._stats["hits"] / total_requests if total_requests > 0 else 0

        return {
            "hits": self._stats["hits"],
            "misses": self._stats["misses"],
            "hit_rate": hit_rate,
            "evictions": self._stats["evictions"],
            "expired": self._stats["expired"],
            "size": len(self._cache),
            "total_requests": total_requests,
        }


class CacheAsidePattern(BaseCachePattern[T]):
    """
    Cache-Aside Pattern Implementation.

    Application manages both cache and data store.
    Cache is populated on demand (lazy loading).
    """

    def __init__(self, default_ttl: float | None = None, max_size: int = 1000):
        super().__init__(default_ttl)
        self.max_size = max_size

    async def get(
        self, key: str, loader: Callable[[], Awaitable[T]], ttl: float | None = None
    ) -> T:
        """Get value from cache or load from data source."""
        # Clean expired entries periodically
        if len(self._cache) > 100:  # Arbitrary threshold
            self._cleanup_expired()

        # Check cache first
        if key in self._cache:
            entry = self._cache[key]
            if not entry.is_expired():
                entry.access()
                self._stats["hits"] += 1
                return entry.value
            else:
                # Entry expired, remove it
                del self._cache[key]
                self._stats["expired"] += 1

        # Cache miss - load from data source
        self._stats["misses"] += 1
        value = await loader()

        # Store in cache
        await self.put(key, value, ttl)

        return value

    async def put(self, key: str, value: T, ttl: float | None = None) -> None:
        """Put value in cache with optional TTL."""
        # Check if we need to evict entries
        if len(self._cache) >= self.max_size:
            await self._evict_lru()

        # Use provided TTL or default
        effective_ttl = ttl if ttl is not None else self.default_ttl

        # Create cache entry
        self._cache[key] = CacheEntry(value=value, ttl=effective_ttl, created_at=time.time())

    async def _evict_lru(self) -> None:
        """Evict least recently used entry."""
        if not self._cache:
            return

        # Find LRU entry
        lru_key = min(self._cache.keys(), key=lambda k: self._cache[k].last_accessed)

        del self._cache[lru_key]
        self._stats["evictions"] += 1


class WriteThroughPattern(BaseCachePattern[T]):
    """
    Write-Through Pattern Implementation.

    Writes go to cache and data store synchronously.
    Ensures cache and data store consistency.
    """

    def __init__(
        self,
        default_ttl: float | None = None,
        writer: Callable[[str, T], Awaitable[None]] | None = None,
    ):
        super().__init__(default_ttl)
        self.writer = writer

    async def get(
        self, key: str, loader: Callable[[], Awaitable[T]], ttl: float | None = None
    ) -> T:
        """Get value from cache or load from data source."""
        self._cleanup_expired()

        # Check cache first
        if key in self._cache:
            entry = self._cache[key]
            if not entry.is_expired():
                entry.access()
                self._stats["hits"] += 1
                return entry.value
            else:
                del self._cache[key]
                self._stats["expired"] += 1

        # Cache miss - load from data source
        self._stats["misses"] += 1
        value = await loader()

        # Store in cache
        await self.put(key, value, ttl)

        return value

    async def put(self, key: str, value: T, ttl: float | None = None) -> None:
        """Put value in cache and write through to data store."""
        # Write to data store first
        if self.writer:
            await self.writer(key, value)

        # Then update cache
        effective_ttl = ttl if ttl is not None else self.default_ttl
        self._cache[key] = CacheEntry(value=value, ttl=effective_ttl, created_at=time.time())


class WriteBehindPattern(BaseCachePattern[T]):
    """
    Write-Behind (Write-Back) Pattern Implementation.

    Writes go to cache immediately, data store asynchronously.
    Provides better write performance but eventual consistency.
    """

    def __init__(
        self,
        default_ttl: float | None = None,
        writer: Callable[[str, T], Awaitable[None]] | None = None,
        write_delay: float = 5.0,
    ):
        super().__init__(default_ttl)
        self.writer = writer
        self.write_delay = write_delay
        self._pending_writes: dict[str, T] = {}
        self._write_task: asyncio.Task | None = None
        self._start_write_behind_task()

    def _start_write_behind_task(self) -> None:
        """Start the background write task."""
        if self._write_task is None or self._write_task.done():
            self._write_task = asyncio.create_task(self._write_behind_worker())

    async def _write_behind_worker(self) -> None:
        """Background worker for writing to data store."""
        while True:
            try:
                await asyncio.sleep(self.write_delay)

                if self._pending_writes and self.writer:
                    # Copy pending writes and clear
                    writes_to_process = dict(self._pending_writes)
                    self._pending_writes.clear()

                    # Process writes
                    for key, value in writes_to_process.items():
                        try:
                            await self.writer(key, value)
                        except Exception as e:
                            # Log error but continue processing
                            print(f"Write-behind error for key {key}: {e}")
                            # Could implement retry logic here

            except asyncio.CancelledError:
                break
            except Exception as e:
                print(f"Write-behind worker error: {e}")
                await asyncio.sleep(1)  # Brief pause before continuing

    async def get(
        self, key: str, loader: Callable[[], Awaitable[T]], ttl: float | None = None
    ) -> T:
        """Get value from cache or load from data source."""
        self._cleanup_expired()

        # Check cache first
        if key in self._cache:
            entry = self._cache[key]
            if not entry.is_expired():
                entry.access()
                self._stats["hits"] += 1
                return entry.value
            else:
                del self._cache[key]
                self._stats["expired"] += 1

        # Cache miss - load from data source
        self._stats["misses"] += 1
        value = await loader()

        # Store in cache
        await self.put(key, value, ttl)

        return value

    async def put(self, key: str, value: T, ttl: float | None = None) -> None:
        """Put value in cache and schedule write-behind."""
        # Update cache immediately
        effective_ttl = ttl if ttl is not None else self.default_ttl
        self._cache[key] = CacheEntry(value=value, ttl=effective_ttl, created_at=time.time())

        # Schedule write-behind
        if self.writer:
            self._pending_writes[key] = value
            self._start_write_behind_task()

    async def flush_pending_writes(self) -> int:
        """Flush all pending writes immediately."""
        if not self._pending_writes or not self.writer:
            return 0

        writes_to_process = dict(self._pending_writes)
        self._pending_writes.clear()

        success_count = 0
        for key, value in writes_to_process.items():
            try:
                await self.writer(key, value)
                success_count += 1
            except Exception as e:
                print(f"Flush write error for key {key}: {e}")

        return success_count

    async def close(self) -> None:
        """Close the cache and flush pending writes."""
        if self._write_task and not self._write_task.done():
            self._write_task.cancel()
            try:
                await self._write_task
            except asyncio.CancelledError:
                pass

        # Flush any remaining writes
        await self.flush_pending_writes()


class CacheManager:
    """
    Cache Manager for multiple cache instances.

    Provides unified interface for different cache patterns and instances.
    """

    def __init__(self):
        self._caches: dict[str, BaseCachePattern] = {}

    def register_cache(self, name: str, cache: BaseCachePattern) -> None:
        """Register a cache instance."""
        self._caches[name] = cache

    def get_cache(self, name: str) -> BaseCachePattern | None:
        """Get cache instance by name."""
        return self._caches.get(name)

    async def clear_all(self) -> dict[str, int]:
        """Clear all registered caches."""
        results = {}
        for name, cache in self._caches.items():
            results[name] = await cache.clear()
        return results

    def get_all_stats(self) -> dict[str, dict[str, Any]]:
        """Get statistics for all caches."""
        stats = {}
        for name, cache in self._caches.items():
            stats[name] = cache.get_stats()
        return stats

    async def close_all(self) -> None:
        """Close all caches (important for write-behind patterns)."""
        for cache in self._caches.values():
            if hasattr(cache, "close"):
                await cache.close()


# Factory function for creating cache patterns
def create_cache_pattern(pattern: CachePattern, **kwargs) -> BaseCachePattern:
    """Factory function to create cache patterns."""
    if pattern == CachePattern.CACHE_ASIDE:
        return CacheAsidePattern(**kwargs)
    elif pattern == CachePattern.WRITE_THROUGH:
        return WriteThroughPattern(**kwargs)
    elif pattern == CachePattern.WRITE_BEHIND:
        return WriteBehindPattern(**kwargs)
    else:
        raise ValueError(f"Unknown cache pattern: {pattern}")


# Example usage patterns
class CacheWarmingStrategy:
    """Strategy for warming up caches proactively."""

    def __init__(self, cache: BaseCachePattern):
        self.cache = cache

    async def warm_cache(self, keys_and_loaders: dict[str, Callable[[], Awaitable[Any]]]) -> int:
        """Warm cache with provided keys and their loaders."""
        warmed_count = 0

        for key, loader in keys_and_loaders.items():
            try:
                await self.cache.get(key, loader)
                warmed_count += 1
            except Exception as e:
                print(f"Error warming cache for key {key}: {e}")

        return warmed_count

    async def warm_cache_batch(
        self, batch_loader: Callable[[], Awaitable[dict[str, Any]]], ttl: float | None = None
    ) -> int:
        """Warm cache with batch-loaded data."""
        try:
            batch_data = await batch_loader()
            warmed_count = 0

            for key, value in batch_data.items():
                await self.cache.put(key, value, ttl)
                warmed_count += 1

            return warmed_count

        except Exception as e:
            print(f"Error in batch cache warming: {e}")
            return 0
