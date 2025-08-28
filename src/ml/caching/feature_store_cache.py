"""
Enhanced Feature Store Caching for Real-time ML Predictions

Advanced caching layer for feature store with intelligent prefetching,
cache warming, and real-time feature serving optimizations.
"""

import asyncio
import json
import time
from collections import defaultdict
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union, Callable

import pandas as pd
import numpy as np

from core.logging import get_logger
from core.caching.redis_cache_manager import RedisCacheManager, get_cache_manager
from core.caching.cache_patterns import CacheAsidePattern, RefreshAheadPattern
from core.caching.redis_streams import publish_cache_warming, EventType, StreamEvent

logger = get_logger(__name__)


class FeatureCacheType(Enum):
    """Types of feature cache entries."""
    INDIVIDUAL_FEATURES = "individual"
    FEATURE_GROUP = "group"
    BATCH_FEATURES = "batch"
    COMPUTED_FEATURES = "computed"
    AGGREGATED_FEATURES = "aggregated"
    HISTORICAL_FEATURES = "historical"


class CacheWarmingStrategy(Enum):
    """Cache warming strategies."""
    EAGER = "eager"  # Pre-load all frequently used features
    LAZY = "lazy"   # Load on first access
    PREDICTIVE = "predictive"  # Pre-load based on usage patterns
    SCHEDULED = "scheduled"  # Load on schedule


@dataclass
class FeatureCacheEntry:
    """Feature cache entry metadata."""
    cache_key: str
    feature_group: str
    feature_names: List[str]
    entity_keys: Dict[str, Any]
    cache_type: FeatureCacheType
    created_at: datetime
    accessed_at: datetime
    expires_at: datetime
    hit_count: int = 0
    computation_cost: float = 0.0  # Time or resources to compute
    data_hash: str = ""
    metadata: Dict[str, Any] = None
    
    def __post_init__(self):
        if self.metadata is None:
            self.metadata = {}
    
    def is_expired(self) -> bool:
        """Check if cache entry is expired."""
        return datetime.utcnow() > self.expires_at
    
    def is_stale(self, staleness_threshold: timedelta = timedelta(minutes=5)) -> bool:
        """Check if cache entry is stale."""
        return datetime.utcnow() - self.accessed_at > staleness_threshold


class FeatureUsageTracker:
    """Tracks feature usage patterns for intelligent caching."""
    
    def __init__(self, cache_manager: Optional[RedisCacheManager] = None):
        self.cache_manager = cache_manager
        self.namespace = "feature_usage"
        
    async def _get_cache_manager(self) -> RedisCacheManager:
        """Get cache manager instance."""
        if self.cache_manager is None:
            self.cache_manager = await get_cache_manager()
        return self.cache_manager
    
    async def record_access(self, feature_group: str, feature_names: List[str],
                          entity_keys: Dict[str, Any], access_time: Optional[datetime] = None):
        """Record feature access for usage tracking."""
        cache_manager = await self._get_cache_manager()
        
        if access_time is None:
            access_time = datetime.utcnow()
        
        timestamp = int(access_time.timestamp())
        
        # Track feature group access frequency
        group_key = f"group_access:{feature_group}"
        await cache_manager.async_redis_client.zadd(
            cache_manager._build_key(group_key, self.namespace),
            {f"{timestamp}:{hash(str(entity_keys))}": timestamp}
        )
        
        # Track individual feature access
        for feature_name in feature_names:
            feature_key = f"feature_access:{feature_group}:{feature_name}"
            await cache_manager.async_redis_client.zadd(
                cache_manager._build_key(feature_key, self.namespace),
                {f"{timestamp}:{hash(str(entity_keys))}": timestamp}
            )
        
        # Track entity access patterns
        entity_key = f"entity_access:{feature_group}"
        entity_hash = hash(str(sorted(entity_keys.items())))
        await cache_manager.async_redis_client.zadd(
            cache_manager._build_key(entity_key, self.namespace),
            {str(entity_hash): timestamp}
        )
        
        # Set TTL for cleanup (30 days)
        await cache_manager.expire(group_key, 2592000, self.namespace)
        for feature_name in feature_names:
            feature_key = f"feature_access:{feature_group}:{feature_name}"
            await cache_manager.expire(feature_key, 2592000, self.namespace)
        await cache_manager.expire(entity_key, 2592000, self.namespace)
    
    async def get_popular_features(self, feature_group: str, 
                                 time_window: timedelta = timedelta(hours=24),
                                 limit: int = 100) -> List[Tuple[str, float]]:
        """Get most popular features in time window."""
        cache_manager = await self._get_cache_manager()
        
        min_timestamp = int((datetime.utcnow() - time_window).timestamp())
        
        # Get popular feature groups
        group_key = f"group_access:{feature_group}"
        popular_accesses = await cache_manager.async_redis_client.zrangebyscore(
            cache_manager._build_key(group_key, self.namespace),
            min_timestamp, "+inf",
            withscores=True
        )
        
        # Count frequencies
        feature_scores = defaultdict(float)
        for access_record, score in popular_accesses:
            feature_scores[feature_group] += 1.0
        
        # Get individual feature popularity
        pattern = cache_manager._build_key(f"feature_access:{feature_group}:*", self.namespace)
        
        if cache_manager.is_cluster:
            all_keys = []
            for node in cache_manager.async_redis_client.get_nodes():
                keys = await node.keys(pattern)
                if keys:
                    all_keys.extend(keys)
        else:
            all_keys = await cache_manager.async_redis_client.keys(pattern)
        
        for key in all_keys:
            feature_name = key.split(":")[-1]
            accesses = await cache_manager.async_redis_client.zcount(
                key, min_timestamp, "+inf"
            )
            feature_scores[feature_name] = float(accesses)
        
        # Sort by popularity
        sorted_features = sorted(feature_scores.items(), key=lambda x: x[1], reverse=True)
        return sorted_features[:limit]
    
    async def get_access_patterns(self, feature_group: str,
                                time_window: timedelta = timedelta(days=7)) -> Dict[str, Any]:
        """Get detailed access patterns for a feature group."""
        cache_manager = await self._get_cache_manager()
        
        min_timestamp = int((datetime.utcnow() - time_window).timestamp())
        
        # Get access timeline
        group_key = f"group_access:{feature_group}"
        timeline = await cache_manager.async_redis_client.zrangebyscore(
            cache_manager._build_key(group_key, self.namespace),
            min_timestamp, "+inf",
            withscores=True
        )
        
        # Analyze patterns
        hourly_pattern = defaultdict(int)
        daily_pattern = defaultdict(int)
        
        for access_record, timestamp in timeline:
            dt = datetime.fromtimestamp(timestamp)
            hourly_pattern[dt.hour] += 1
            daily_pattern[dt.weekday()] += 1
        
        return {
            "total_accesses": len(timeline),
            "hourly_pattern": dict(hourly_pattern),
            "daily_pattern": dict(daily_pattern),
            "peak_hour": max(hourly_pattern.items(), key=lambda x: x[1])[0] if hourly_pattern else 0,
            "peak_day": max(daily_pattern.items(), key=lambda x: x[1])[0] if daily_pattern else 0
        }


class FeatureStoreCache:
    """Advanced feature store caching with intelligent warming and invalidation."""
    
    def __init__(self, cache_manager: Optional[RedisCacheManager] = None,
                 default_ttl: int = 900,  # 15 minutes
                 enable_usage_tracking: bool = True):
        self.cache_manager = cache_manager
        self.default_ttl = default_ttl
        self.namespace = "feature_store"
        self.metadata_namespace = "feature_store_meta"
        
        # Usage tracking
        self.usage_tracker = FeatureUsageTracker(cache_manager) if enable_usage_tracking else None
        
        # Cache patterns
        self.cache_aside = CacheAsidePattern(cache_manager, default_ttl)
        self.cache_aside.namespace = self.namespace
        
        self.refresh_ahead = RefreshAheadPattern(
            cache_manager, default_ttl=default_ttl, refresh_threshold=0.7
        )
        self.refresh_ahead.namespace = self.namespace
        
        # Statistics
        self.stats = {
            "hits": 0,
            "misses": 0,
            "cache_size": 0,
            "warming_operations": 0
        }
    
    async def _get_cache_manager(self) -> RedisCacheManager:
        """Get cache manager instance."""
        if self.cache_manager is None:
            self.cache_manager = await get_cache_manager()
        return self.cache_manager
    
    def _build_feature_key(self, feature_group: str, entity_keys: Dict[str, Any],
                          feature_names: Optional[List[str]] = None,
                          version: str = "latest") -> str:
        """Build cache key for feature data."""
        # Sort entity keys for consistent hashing
        entity_str = ":".join(f"{k}={v}" for k, v in sorted(entity_keys.items()))
        
        if feature_names:
            features_str = ":".join(sorted(feature_names))
            return f"features:{feature_group}:{version}:{entity_str}:{features_str}"
        else:
            return f"features:{feature_group}:{version}:{entity_str}"
    
    def _build_group_key(self, feature_group: str, version: str = "latest") -> str:
        """Build cache key for feature group metadata."""
        return f"group_meta:{feature_group}:{version}"
    
    async def cache_features(self, feature_group: str, entity_keys: Dict[str, Any],
                           features: Dict[str, Any], version: str = "latest",
                           ttl: Optional[int] = None,
                           computation_cost: float = 0.0,
                           metadata: Optional[Dict[str, Any]] = None) -> str:
        """Cache feature values for an entity."""
        cache_manager = await self._get_cache_manager()
        
        cache_key = self._build_feature_key(feature_group, entity_keys, None, version)
        cache_ttl = ttl or self.default_ttl
        
        # Prepare cache data
        cache_data = {
            "features": features,
            "entity_keys": entity_keys,
            "feature_group": feature_group,
            "version": version,
            "cached_at": datetime.utcnow().isoformat(),
            "computation_cost": computation_cost,
            "metadata": metadata or {}
        }
        
        # Store features
        success = await cache_manager.set(
            cache_key, cache_data, cache_ttl, self.namespace
        )
        
        if success:
            # Store metadata
            cache_entry = FeatureCacheEntry(
                cache_key=cache_key,
                feature_group=feature_group,
                feature_names=list(features.keys()),
                entity_keys=entity_keys,
                cache_type=FeatureCacheType.INDIVIDUAL_FEATURES,
                created_at=datetime.utcnow(),
                accessed_at=datetime.utcnow(),
                expires_at=datetime.utcnow() + timedelta(seconds=cache_ttl),
                computation_cost=computation_cost,
                metadata=metadata or {}
            )
            
            await self._store_cache_metadata(cache_entry)
            
            logger.debug(f"Cached features for {feature_group} with key {cache_key}")
            self.stats["cache_size"] += 1
        
        return cache_key if success else ""
    
    async def get_features(self, feature_group: str, entity_keys: Dict[str, Any],
                         feature_names: Optional[List[str]] = None,
                         version: str = "latest",
                         fallback_loader: Optional[Callable] = None) -> Optional[Dict[str, Any]]:
        """Get cached features or load them using fallback."""
        cache_key = self._build_feature_key(feature_group, entity_keys, feature_names, version)
        
        # Try cache first
        cached_data = await self.cache_aside.get(cache_key, fallback_loader)
        
        if cached_data:
            self.stats["hits"] += 1
            
            # Record usage for analytics
            if self.usage_tracker:
                await self.usage_tracker.record_access(
                    feature_group, feature_names or list(cached_data.get("features", {}).keys()),
                    entity_keys
                )
            
            # Update access time in metadata
            await self._update_access_time(cache_key)
            
            # Return features only
            return cached_data.get("features", {})
        
        self.stats["misses"] += 1
        return None
    
    async def get_features_batch(self, feature_group: str, 
                               entity_keys_list: List[Dict[str, Any]],
                               feature_names: Optional[List[str]] = None,
                               version: str = "latest",
                               fallback_loader: Optional[Callable] = None) -> Dict[str, Dict[str, Any]]:
        """Get features for multiple entities in batch."""
        cache_manager = await self._get_cache_manager()
        
        # Build cache keys for all entities
        cache_keys = [
            self._build_feature_key(feature_group, entity_keys, feature_names, version)
            for entity_keys in entity_keys_list
        ]
        
        # Batch get from cache
        cached_results = await cache_manager.get_multi(cache_keys, self.namespace)
        
        results = {}
        missing_entities = []
        
        # Process cached results
        for i, (cache_key, cached_data) in enumerate(zip(cache_keys, cached_results.values())):
            entity_keys = entity_keys_list[i]
            entity_id = self._get_entity_id(entity_keys)
            
            if cached_data:
                self.stats["hits"] += 1
                results[entity_id] = cached_data.get("features", {})
                
                # Update access time
                await self._update_access_time(cache_key)
            else:
                self.stats["misses"] += 1
                missing_entities.append((entity_id, entity_keys))
        
        # Load missing entities if fallback loader provided
        if missing_entities and fallback_loader:
            missing_entity_keys = [ek for _, ek in missing_entities]
            
            if asyncio.iscoroutinefunction(fallback_loader):
                loaded_features = await fallback_loader(
                    feature_group, missing_entity_keys, feature_names, version
                )
            else:
                loaded_features = fallback_loader(
                    feature_group, missing_entity_keys, feature_names, version
                )
            
            # Cache loaded features
            if isinstance(loaded_features, dict):
                for entity_id, features in loaded_features.items():
                    if features:
                        # Find corresponding entity_keys
                        entity_keys = next(
                            ek for eid, ek in missing_entities if eid == entity_id
                        )
                        await self.cache_features(
                            feature_group, entity_keys, features, version
                        )
                        results[entity_id] = features
        
        # Record batch usage
        if self.usage_tracker and results:
            for entity_keys in entity_keys_list:
                await self.usage_tracker.record_access(
                    feature_group, feature_names or [], entity_keys
                )
        
        return results
    
    def _get_entity_id(self, entity_keys: Dict[str, Any]) -> str:
        """Generate entity ID from entity keys."""
        sorted_items = sorted(entity_keys.items())
        return ":".join(f"{k}={v}" for k, v in sorted_items)
    
    async def cache_feature_group_metadata(self, feature_group: str, version: str,
                                         metadata: Dict[str, Any],
                                         ttl: Optional[int] = None) -> bool:
        """Cache feature group metadata."""
        cache_manager = await self._get_cache_manager()
        
        cache_key = self._build_group_key(feature_group, version)
        cache_ttl = ttl or (self.default_ttl * 4)  # Longer TTL for metadata
        
        return await cache_manager.set(
            cache_key, metadata, cache_ttl, self.namespace
        )
    
    async def get_feature_group_metadata(self, feature_group: str, 
                                       version: str = "latest") -> Optional[Dict[str, Any]]:
        """Get cached feature group metadata."""
        cache_manager = await self._get_cache_manager()
        
        cache_key = self._build_group_key(feature_group, version)
        return await cache_manager.get(cache_key, self.namespace)
    
    async def warm_cache(self, feature_group: str, entity_keys_list: List[Dict[str, Any]],
                        feature_names: Optional[List[str]] = None,
                        version: str = "latest",
                        loader_func: Optional[Callable] = None,
                        strategy: CacheWarmingStrategy = CacheWarmingStrategy.EAGER) -> int:
        """Warm cache with feature data."""
        if not loader_func:
            logger.warning("No loader function provided for cache warming")
            return 0
        
        warmed_count = 0
        
        try:
            if strategy == CacheWarmingStrategy.EAGER:
                # Load all features immediately
                if asyncio.iscoroutinefunction(loader_func):
                    loaded_data = await loader_func(
                        feature_group, entity_keys_list, feature_names, version
                    )
                else:
                    loaded_data = loader_func(
                        feature_group, entity_keys_list, feature_names, version
                    )
                
                # Cache loaded data
                if isinstance(loaded_data, dict):
                    for entity_id, features in loaded_data.items():
                        if features:
                            # Find corresponding entity_keys
                            entity_keys = next(
                                ek for ek in entity_keys_list 
                                if self._get_entity_id(ek) == entity_id
                            )
                            await self.cache_features(
                                feature_group, entity_keys, features, version
                            )
                            warmed_count += 1
            
            elif strategy == CacheWarmingStrategy.PREDICTIVE:
                # Use usage patterns to determine what to warm
                if self.usage_tracker:
                    popular_features = await self.usage_tracker.get_popular_features(
                        feature_group, timedelta(hours=24), limit=50
                    )
                    
                    # Focus on most popular features for warming
                    popular_feature_names = [name for name, _ in popular_features 
                                           if name != feature_group]
                    
                    if popular_feature_names:
                        if asyncio.iscoroutinefunction(loader_func):
                            loaded_data = await loader_func(
                                feature_group, entity_keys_list[:10],  # Limit entities
                                popular_feature_names[:5], version  # Top 5 features
                            )
                        else:
                            loaded_data = loader_func(
                                feature_group, entity_keys_list[:10],
                                popular_feature_names[:5], version
                            )
                        
                        # Cache loaded data
                        if isinstance(loaded_data, dict):
                            for entity_id, features in loaded_data.items():
                                if features:
                                    entity_keys = next(
                                        ek for ek in entity_keys_list[:10]
                                        if self._get_entity_id(ek) == entity_id
                                    )
                                    await self.cache_features(
                                        feature_group, entity_keys, features, version
                                    )
                                    warmed_count += 1
            
            self.stats["warming_operations"] += 1
            logger.info(f"Warmed cache with {warmed_count} feature entries")
            
            # Publish warming event
            await publish_cache_warming(
                f"feature_group:{feature_group}",
                {"warmed_count": warmed_count, "strategy": strategy.value},
                ttl=self.default_ttl,
                source="feature_store_cache"
            )
            
        except Exception as e:
            logger.error(f"Error warming cache: {e}")
        
        return warmed_count
    
    async def invalidate_features(self, feature_group: str, 
                                entity_keys: Optional[Dict[str, Any]] = None,
                                version: str = "latest") -> int:
        """Invalidate cached features."""
        cache_manager = await self._get_cache_manager()
        
        if entity_keys:
            # Invalidate specific entity
            cache_key = self._build_feature_key(feature_group, entity_keys, None, version)
            success = await cache_manager.delete(cache_key, self.namespace)
            
            # Also invalidate metadata
            await self._delete_cache_metadata(cache_key)
            
            return 1 if success else 0
        else:
            # Invalidate entire feature group
            pattern = f"features:{feature_group}:{version}:*"
            count = await cache_manager.invalidate_pattern(pattern, self.namespace)
            
            # Also invalidate metadata
            meta_pattern = f"meta:features:{feature_group}:{version}:*"
            await cache_manager.invalidate_pattern(meta_pattern, self.metadata_namespace)
            
            logger.info(f"Invalidated {count} cache entries for feature group {feature_group}")
            return count
    
    async def _store_cache_metadata(self, cache_entry: FeatureCacheEntry):
        """Store cache entry metadata."""
        cache_manager = await self._get_cache_manager()
        
        metadata_key = f"meta:{cache_entry.cache_key}"
        
        # Convert to dict for storage
        entry_dict = asdict(cache_entry)
        entry_dict["created_at"] = entry_dict["created_at"].isoformat()
        entry_dict["accessed_at"] = entry_dict["accessed_at"].isoformat()
        entry_dict["expires_at"] = entry_dict["expires_at"].isoformat()
        entry_dict["cache_type"] = entry_dict["cache_type"].value
        
        await cache_manager.set(
            metadata_key, entry_dict, 
            int((cache_entry.expires_at - datetime.utcnow()).total_seconds()) + 300,
            self.metadata_namespace
        )
    
    async def _update_access_time(self, cache_key: str):
        """Update access time for cache entry."""
        cache_manager = await self._get_cache_manager()
        
        metadata_key = f"meta:{cache_key}"
        current_metadata = await cache_manager.get(metadata_key, self.metadata_namespace)
        
        if current_metadata:
            current_metadata["accessed_at"] = datetime.utcnow().isoformat()
            current_metadata["hit_count"] = current_metadata.get("hit_count", 0) + 1
            
            await cache_manager.set(
                metadata_key, current_metadata,
                namespace=self.metadata_namespace
            )
    
    async def _delete_cache_metadata(self, cache_key: str):
        """Delete cache entry metadata."""
        cache_manager = await self._get_cache_manager()
        
        metadata_key = f"meta:{cache_key}"
        await cache_manager.delete(metadata_key, self.metadata_namespace)
    
    async def get_cache_statistics(self) -> Dict[str, Any]:
        """Get comprehensive cache statistics."""
        cache_manager = await self._get_cache_manager()
        
        stats = {
            "basic_stats": self.stats.copy(),
            "feature_groups": {},
            "cache_health": {
                "hit_rate": 0.0,
                "avg_computation_cost": 0.0,
                "memory_usage": 0
            }
        }
        
        try:
            # Calculate hit rate
            total_requests = self.stats["hits"] + self.stats["misses"]
            if total_requests > 0:
                stats["cache_health"]["hit_rate"] = self.stats["hits"] / total_requests
            
            # Get detailed stats from metadata
            meta_pattern = cache_manager._build_key("meta:features:*", self.metadata_namespace)
            
            if cache_manager.is_cluster:
                all_keys = []
                for node in cache_manager.async_redis_client.get_nodes():
                    keys = await node.keys(meta_pattern)
                    if keys:
                        all_keys.extend(keys)
            else:
                all_keys = await cache_manager.async_redis_client.keys(meta_pattern)
            
            total_cost = 0.0
            cost_count = 0
            
            for key in all_keys[:100]:  # Limit for performance
                try:
                    metadata = await cache_manager.get(
                        key.replace(cache_manager.key_prefix + self.metadata_namespace + ":", ""),
                        self.metadata_namespace
                    )
                    
                    if metadata:
                        feature_group = metadata.get("feature_group", "unknown")
                        
                        if feature_group not in stats["feature_groups"]:
                            stats["feature_groups"][feature_group] = {
                                "entries": 0,
                                "total_hits": 0,
                                "avg_cost": 0.0
                            }
                        
                        stats["feature_groups"][feature_group]["entries"] += 1
                        stats["feature_groups"][feature_group]["total_hits"] += metadata.get("hit_count", 0)
                        
                        cost = metadata.get("computation_cost", 0.0)
                        if cost > 0:
                            total_cost += cost
                            cost_count += 1
                            stats["feature_groups"][feature_group]["avg_cost"] += cost
                
                except Exception as e:
                    logger.error(f"Error processing metadata key {key}: {e}")
            
            # Calculate average computation cost
            if cost_count > 0:
                stats["cache_health"]["avg_computation_cost"] = total_cost / cost_count
            
            # Calculate average costs per feature group
            for group_stats in stats["feature_groups"].values():
                if group_stats["entries"] > 0:
                    group_stats["avg_cost"] /= group_stats["entries"]
        
        except Exception as e:
            logger.error(f"Error getting cache statistics: {e}")
            stats["error"] = str(e)
        
        return stats
    
    async def cleanup_expired_cache(self) -> int:
        """Clean up expired cache entries."""
        cache_manager = await self._get_cache_manager()
        
        try:
            # Get all metadata keys
            meta_pattern = cache_manager._build_key("meta:features:*", self.metadata_namespace)
            
            if cache_manager.is_cluster:
                all_keys = []
                for node in cache_manager.async_redis_client.get_nodes():
                    keys = await node.keys(meta_pattern)
                    if keys:
                        all_keys.extend(keys)
            else:
                all_keys = await cache_manager.async_redis_client.keys(meta_pattern)
            
            cleaned_count = 0
            
            for key in all_keys:
                try:
                    metadata = await cache_manager.get(
                        key.replace(cache_manager.key_prefix + self.metadata_namespace + ":", ""),
                        self.metadata_namespace
                    )
                    
                    if metadata:
                        expires_at = datetime.fromisoformat(metadata["expires_at"])
                        
                        if datetime.utcnow() > expires_at:
                            # Delete both data and metadata
                            cache_key = metadata["cache_key"]
                            await cache_manager.delete(cache_key, self.namespace)
                            await cache_manager.delete(f"meta:{cache_key}", self.metadata_namespace)
                            cleaned_count += 1
                
                except Exception as e:
                    logger.error(f"Error cleaning cache entry {key}: {e}")
            
            logger.info(f"Cleaned up {cleaned_count} expired feature cache entries")
            return cleaned_count
            
        except Exception as e:
            logger.error(f"Error during feature cache cleanup: {e}")
            return 0


# Global feature store cache instance
_feature_store_cache: Optional[FeatureStoreCache] = None


async def get_feature_store_cache() -> FeatureStoreCache:
    """Get or create global feature store cache instance."""
    global _feature_store_cache
    if _feature_store_cache is None:
        _feature_store_cache = FeatureStoreCache()
    return _feature_store_cache


# Convenience decorators and functions

def cache_features(feature_group: str, ttl: Optional[int] = None):
    """Decorator for caching feature computations."""
    def decorator(func):
        async def wrapper(*args, **kwargs):
            feature_cache = await get_feature_store_cache()
            
            # Extract entity keys from arguments (assuming first arg)
            if args and isinstance(args[0], dict):
                entity_keys = args[0]
            else:
                entity_keys = {"id": hash(str(args) + str(kwargs))}
            
            # Try to get cached features
            cached_features = await feature_cache.get_features(
                feature_group, entity_keys
            )
            
            if cached_features:
                return cached_features
            
            # Compute features
            start_time = time.time()
            
            if asyncio.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                result = func(*args, **kwargs)
            
            computation_time = time.time() - start_time
            
            # Cache result
            if isinstance(result, dict):
                await feature_cache.cache_features(
                    feature_group, entity_keys, result,
                    ttl=ttl, computation_cost=computation_time
                )
            
            return result
        
        return wrapper
    return decorator