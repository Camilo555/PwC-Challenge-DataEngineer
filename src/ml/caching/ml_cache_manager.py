"""
ML Model Result Caching with TTL Management

Advanced caching system for ML model predictions, training results,
feature computations, and model artifacts with intelligent TTL management.
"""

import asyncio
import hashlib
import json
import pickle
import time
import uuid
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Union, Callable

import numpy as np
import pandas as pd

from core.logging import get_logger
from core.caching.redis_cache_manager import RedisCacheManager, get_cache_manager
from core.caching.cache_patterns import CacheAsidePattern, WriteThroughPattern
from core.caching.redis_streams import publish_cache_invalidation, EventType, StreamEvent

logger = get_logger(__name__)


class MLCacheType(Enum):
    """ML cache entry types."""
    PREDICTION = "prediction"
    BATCH_PREDICTION = "batch_prediction"
    MODEL_ARTIFACT = "model_artifact"
    FEATURE_COMPUTATION = "feature_computation"
    TRAINING_RESULT = "training_result"
    MODEL_METADATA = "model_metadata"
    FEATURE_STORE_DATA = "feature_store_data"
    EXPERIMENT_RESULT = "experiment_result"
    MODEL_METRICS = "model_metrics"


class CacheStrategy(Enum):
    """Cache invalidation strategies."""
    TIME_BASED = "time_based"
    MODEL_VERSION_BASED = "model_version_based"
    DATA_DRIFT_BASED = "data_drift_based"
    PERFORMANCE_BASED = "performance_based"
    MANUAL = "manual"


@dataclass
class MLCacheEntry:
    """ML cache entry metadata."""
    cache_key: str
    cache_type: MLCacheType
    model_id: str
    model_version: str
    created_at: datetime
    expires_at: datetime
    data_hash: str
    metadata: Dict[str, Any]
    hit_count: int = 0
    size_bytes: int = 0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for storage."""
        return {
            "cache_key": self.cache_key,
            "cache_type": self.cache_type.value,
            "model_id": self.model_id,
            "model_version": self.model_version,
            "created_at": self.created_at.isoformat(),
            "expires_at": self.expires_at.isoformat(),
            "data_hash": self.data_hash,
            "metadata": json.dumps(self.metadata, default=str),
            "hit_count": str(self.hit_count),
            "size_bytes": str(self.size_bytes)
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MLCacheEntry':
        """Create from dictionary."""
        return cls(
            cache_key=data["cache_key"],
            cache_type=MLCacheType(data["cache_type"]),
            model_id=data["model_id"],
            model_version=data["model_version"],
            created_at=datetime.fromisoformat(data["created_at"]),
            expires_at=datetime.fromisoformat(data["expires_at"]),
            data_hash=data["data_hash"],
            metadata=json.loads(data.get("metadata", "{}")),
            hit_count=int(data.get("hit_count", 0)),
            size_bytes=int(data.get("size_bytes", 0))
        )
    
    def is_expired(self) -> bool:
        """Check if cache entry is expired."""
        return datetime.utcnow() > self.expires_at
    
    def increment_hit_count(self):
        """Increment hit count."""
        self.hit_count += 1


class TTLManager:
    """Intelligent TTL management for ML cache entries."""
    
    def __init__(self):
        self.base_ttls = {
            MLCacheType.PREDICTION: 3600,  # 1 hour
            MLCacheType.BATCH_PREDICTION: 7200,  # 2 hours
            MLCacheType.MODEL_ARTIFACT: 86400,  # 1 day
            MLCacheType.FEATURE_COMPUTATION: 1800,  # 30 minutes
            MLCacheType.TRAINING_RESULT: 172800,  # 2 days
            MLCacheType.MODEL_METADATA: 21600,  # 6 hours
            MLCacheType.FEATURE_STORE_DATA: 900,  # 15 minutes
            MLCacheType.EXPERIMENT_RESULT: 259200,  # 3 days
            MLCacheType.MODEL_METRICS: 3600,  # 1 hour
        }
    
    def calculate_ttl(self, cache_type: MLCacheType, 
                     model_performance: Optional[float] = None,
                     data_freshness: Optional[float] = None,
                     usage_frequency: Optional[float] = None,
                     custom_factors: Optional[Dict[str, float]] = None) -> int:
        """
        Calculate intelligent TTL based on various factors.
        
        Args:
            cache_type: Type of ML cache entry
            model_performance: Model performance score (0-1)
            data_freshness: Data freshness score (0-1, 1=very fresh)
            usage_frequency: Usage frequency score (0-1, 1=very frequent)
            custom_factors: Additional factors to consider
        """
        base_ttl = self.base_ttls.get(cache_type, 3600)
        
        # Start with base TTL
        adjusted_ttl = base_ttl
        
        # Adjust based on model performance
        if model_performance is not None:
            # Higher performance -> longer TTL
            performance_factor = 0.5 + (model_performance * 0.5)  # 0.5 to 1.0
            adjusted_ttl *= performance_factor
        
        # Adjust based on data freshness
        if data_freshness is not None:
            # Fresher data -> longer TTL
            freshness_factor = 0.7 + (data_freshness * 0.6)  # 0.7 to 1.3
            adjusted_ttl *= freshness_factor
        
        # Adjust based on usage frequency
        if usage_frequency is not None:
            # More frequent usage -> longer TTL
            frequency_factor = 0.8 + (usage_frequency * 0.4)  # 0.8 to 1.2
            adjusted_ttl *= frequency_factor
        
        # Apply custom factors
        if custom_factors:
            for factor_name, factor_value in custom_factors.items():
                if factor_name == "model_complexity" and factor_value > 0.8:
                    # Complex models benefit from longer caching
                    adjusted_ttl *= 1.2
                elif factor_name == "data_volatility" and factor_value > 0.7:
                    # Volatile data needs shorter TTL
                    adjusted_ttl *= 0.7
                elif factor_name == "business_criticality" and factor_value > 0.9:
                    # Critical business data gets shorter TTL for freshness
                    adjusted_ttl *= 0.8
        
        # Ensure reasonable bounds
        min_ttl = base_ttl * 0.1  # At least 10% of base
        max_ttl = base_ttl * 3.0  # At most 3x base
        
        return int(max(min_ttl, min(max_ttl, adjusted_ttl)))


class DataHasher:
    """Utility for creating consistent hashes of ML data."""
    
    @staticmethod
    def hash_dataframe(df: pd.DataFrame) -> str:
        """Create hash for pandas DataFrame."""
        return hashlib.md5(
            pd.util.hash_pandas_object(df, index=True).values
        ).hexdigest()
    
    @staticmethod
    def hash_numpy_array(arr: np.ndarray) -> str:
        """Create hash for numpy array."""
        return hashlib.md5(arr.tobytes()).hexdigest()
    
    @staticmethod
    def hash_dict(data: Dict[str, Any]) -> str:
        """Create hash for dictionary."""
        json_str = json.dumps(data, sort_keys=True, default=str)
        return hashlib.md5(json_str.encode()).hexdigest()
    
    @staticmethod
    def hash_mixed_data(data: Any) -> str:
        """Create hash for mixed data types."""
        if isinstance(data, pd.DataFrame):
            return DataHasher.hash_dataframe(data)
        elif isinstance(data, np.ndarray):
            return DataHasher.hash_numpy_array(data)
        elif isinstance(data, dict):
            return DataHasher.hash_dict(data)
        elif isinstance(data, (list, tuple)):
            # Convert to string representation for hashing
            str_repr = str(data)
            return hashlib.md5(str_repr.encode()).hexdigest()
        else:
            str_repr = str(data)
            return hashlib.md5(str_repr.encode()).hexdigest()


class MLCacheManager:
    """Advanced ML caching manager with intelligent TTL and invalidation."""
    
    def __init__(self, cache_manager: Optional[RedisCacheManager] = None):
        self.cache_manager = cache_manager
        self.ttl_manager = TTLManager()
        self.namespace = "ml_cache"
        self.metadata_namespace = "ml_cache_meta"
        self.stats = {
            "hits": 0,
            "misses": 0,
            "evictions": 0,
            "invalidations": 0
        }
        
    async def _get_cache_manager(self) -> RedisCacheManager:
        """Get cache manager instance."""
        if self.cache_manager is None:
            self.cache_manager = await get_cache_manager()
        return self.cache_manager
    
    def _build_cache_key(self, model_id: str, input_hash: str, 
                        cache_type: MLCacheType, suffix: str = "") -> str:
        """Build cache key for ML data."""
        key_parts = [model_id, cache_type.value, input_hash]
        if suffix:
            key_parts.append(suffix)
        return ":".join(key_parts)
    
    async def _serialize_ml_data(self, data: Any) -> bytes:
        """Serialize ML data for Redis storage."""
        try:
            if isinstance(data, (pd.DataFrame, pd.Series)):
                # Use pickle for pandas objects
                return pickle.dumps(data)
            elif isinstance(data, np.ndarray):
                # Use numpy's save format
                return pickle.dumps(data)
            else:
                # Use pickle for other objects
                return pickle.dumps(data)
        except Exception as e:
            logger.error(f"Serialization error: {e}")
            raise
    
    async def _deserialize_ml_data(self, data: bytes) -> Any:
        """Deserialize ML data from Redis storage."""
        try:
            return pickle.loads(data)
        except Exception as e:
            logger.error(f"Deserialization error: {e}")
            raise
    
    async def cache_prediction(self, model_id: str, model_version: str,
                             input_data: Any, prediction: Any,
                             confidence: Optional[float] = None,
                             ttl_factors: Optional[Dict[str, float]] = None,
                             metadata: Optional[Dict[str, Any]] = None) -> str:
        """
        Cache model prediction result.
        
        Args:
            model_id: Model identifier
            model_version: Model version
            input_data: Input data used for prediction
            prediction: Prediction result
            confidence: Prediction confidence score
            ttl_factors: Factors for TTL calculation
            metadata: Additional metadata
            
        Returns:
            Cache key
        """
        cache_manager = await self._get_cache_manager()
        
        # Generate input hash
        input_hash = DataHasher.hash_mixed_data(input_data)
        
        # Build cache key
        cache_key = self._build_cache_key(model_id, input_hash, 
                                        MLCacheType.PREDICTION)
        
        # Calculate TTL
        ttl = self.ttl_manager.calculate_ttl(
            MLCacheType.PREDICTION,
            model_performance=confidence,
            custom_factors=ttl_factors
        )
        
        # Prepare cache data
        cache_data = {
            "prediction": prediction,
            "confidence": confidence,
            "model_version": model_version,
            "input_hash": input_hash,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Serialize and store
        serialized_data = await self._serialize_ml_data(cache_data)
        
        # Store in Redis
        await cache_manager.async_redis_client.setex(
            cache_manager._build_key(cache_key, self.namespace),
            ttl,
            serialized_data
        )
        
        # Store metadata
        cache_entry = MLCacheEntry(
            cache_key=cache_key,
            cache_type=MLCacheType.PREDICTION,
            model_id=model_id,
            model_version=model_version,
            created_at=datetime.utcnow(),
            expires_at=datetime.utcnow() + timedelta(seconds=ttl),
            data_hash=input_hash,
            metadata=metadata or {},
            size_bytes=len(serialized_data)
        )
        
        await self._store_cache_metadata(cache_entry)
        
        logger.debug(f"Cached prediction for model {model_id}, TTL: {ttl}s")
        return cache_key
    
    async def get_cached_prediction(self, model_id: str, input_data: Any) -> Optional[Dict[str, Any]]:
        """Get cached prediction result."""
        cache_manager = await self._get_cache_manager()
        
        # Generate input hash
        input_hash = DataHasher.hash_mixed_data(input_data)
        
        # Build cache key
        cache_key = self._build_cache_key(model_id, input_hash, 
                                        MLCacheType.PREDICTION)
        
        try:
            # Get from Redis
            serialized_data = await cache_manager.async_redis_client.get(
                cache_manager._build_key(cache_key, self.namespace)
            )
            
            if serialized_data is None:
                self.stats["misses"] += 1
                return None
            
            # Deserialize data
            cache_data = await self._deserialize_ml_data(serialized_data)
            
            # Update hit count in metadata
            await self._update_hit_count(cache_key)
            
            self.stats["hits"] += 1
            logger.debug(f"Cache hit for prediction {cache_key}")
            
            return cache_data
            
        except Exception as e:
            logger.error(f"Error retrieving cached prediction: {e}")
            self.stats["misses"] += 1
            return None
    
    async def cache_batch_predictions(self, model_id: str, model_version: str,
                                    batch_input: Any, batch_predictions: Any,
                                    batch_metadata: Optional[Dict[str, Any]] = None,
                                    ttl_factors: Optional[Dict[str, float]] = None) -> str:
        """Cache batch prediction results."""
        cache_manager = await self._get_cache_manager()
        
        # Generate batch hash
        batch_hash = DataHasher.hash_mixed_data(batch_input)
        
        # Build cache key
        cache_key = self._build_cache_key(model_id, batch_hash, 
                                        MLCacheType.BATCH_PREDICTION)
        
        # Calculate TTL
        ttl = self.ttl_manager.calculate_ttl(
            MLCacheType.BATCH_PREDICTION,
            custom_factors=ttl_factors
        )
        
        # Prepare cache data
        cache_data = {
            "predictions": batch_predictions,
            "model_version": model_version,
            "batch_size": len(batch_predictions) if hasattr(batch_predictions, '__len__') else 1,
            "batch_hash": batch_hash,
            "metadata": batch_metadata or {},
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Serialize and store
        serialized_data = await self._serialize_ml_data(cache_data)
        
        await cache_manager.async_redis_client.setex(
            cache_manager._build_key(cache_key, self.namespace),
            ttl,
            serialized_data
        )
        
        # Store metadata
        cache_entry = MLCacheEntry(
            cache_key=cache_key,
            cache_type=MLCacheType.BATCH_PREDICTION,
            model_id=model_id,
            model_version=model_version,
            created_at=datetime.utcnow(),
            expires_at=datetime.utcnow() + timedelta(seconds=ttl),
            data_hash=batch_hash,
            metadata=batch_metadata or {},
            size_bytes=len(serialized_data)
        )
        
        await self._store_cache_metadata(cache_entry)
        
        logger.info(f"Cached batch predictions for model {model_id}, size: {len(serialized_data)} bytes")
        return cache_key
    
    async def cache_model_artifact(self, model_id: str, model_version: str,
                                 artifact_type: str, artifact_data: Any,
                                 ttl_override: Optional[int] = None) -> str:
        """Cache model artifacts (weights, configs, etc.)."""
        cache_manager = await self._get_cache_manager()
        
        # Generate artifact hash
        artifact_hash = DataHasher.hash_mixed_data(artifact_data)
        
        # Build cache key
        cache_key = self._build_cache_key(model_id, artifact_hash, 
                                        MLCacheType.MODEL_ARTIFACT, artifact_type)
        
        # Calculate TTL
        ttl = ttl_override or self.ttl_manager.calculate_ttl(MLCacheType.MODEL_ARTIFACT)
        
        # Serialize and store
        serialized_data = await self._serialize_ml_data(artifact_data)
        
        await cache_manager.async_redis_client.setex(
            cache_manager._build_key(cache_key, self.namespace),
            ttl,
            serialized_data
        )
        
        # Store metadata
        cache_entry = MLCacheEntry(
            cache_key=cache_key,
            cache_type=MLCacheType.MODEL_ARTIFACT,
            model_id=model_id,
            model_version=model_version,
            created_at=datetime.utcnow(),
            expires_at=datetime.utcnow() + timedelta(seconds=ttl),
            data_hash=artifact_hash,
            metadata={"artifact_type": artifact_type},
            size_bytes=len(serialized_data)
        )
        
        await self._store_cache_metadata(cache_entry)
        
        logger.info(f"Cached model artifact {artifact_type} for model {model_id}")
        return cache_key
    
    async def cache_feature_computation(self, feature_name: str, input_data: Any,
                                      computed_features: Any,
                                      computation_time: Optional[float] = None,
                                      ttl_factors: Optional[Dict[str, float]] = None) -> str:
        """Cache computed feature values."""
        cache_manager = await self._get_cache_manager()
        
        # Generate input hash
        input_hash = DataHasher.hash_mixed_data(input_data)
        
        # Build cache key
        cache_key = self._build_cache_key(feature_name, input_hash, 
                                        MLCacheType.FEATURE_COMPUTATION)
        
        # Calculate TTL
        ttl = self.ttl_manager.calculate_ttl(
            MLCacheType.FEATURE_COMPUTATION,
            custom_factors=ttl_factors
        )
        
        # Prepare cache data
        cache_data = {
            "features": computed_features,
            "feature_name": feature_name,
            "input_hash": input_hash,
            "computation_time": computation_time,
            "timestamp": datetime.utcnow().isoformat()
        }
        
        # Serialize and store
        serialized_data = await self._serialize_ml_data(cache_data)
        
        await cache_manager.async_redis_client.setex(
            cache_manager._build_key(cache_key, self.namespace),
            ttl,
            serialized_data
        )
        
        # Store metadata
        cache_entry = MLCacheEntry(
            cache_key=cache_key,
            cache_type=MLCacheType.FEATURE_COMPUTATION,
            model_id=feature_name,
            model_version="1.0",
            created_at=datetime.utcnow(),
            expires_at=datetime.utcnow() + timedelta(seconds=ttl),
            data_hash=input_hash,
            metadata={"computation_time": computation_time},
            size_bytes=len(serialized_data)
        )
        
        await self._store_cache_metadata(cache_entry)
        
        logger.debug(f"Cached feature computation for {feature_name}")
        return cache_key
    
    async def get_cached_features(self, feature_name: str, input_data: Any) -> Optional[Dict[str, Any]]:
        """Get cached feature computation result."""
        cache_manager = await self._get_cache_manager()
        
        # Generate input hash
        input_hash = DataHasher.hash_mixed_data(input_data)
        
        # Build cache key
        cache_key = self._build_cache_key(feature_name, input_hash, 
                                        MLCacheType.FEATURE_COMPUTATION)
        
        try:
            # Get from Redis
            serialized_data = await cache_manager.async_redis_client.get(
                cache_manager._build_key(cache_key, self.namespace)
            )
            
            if serialized_data is None:
                self.stats["misses"] += 1
                return None
            
            # Deserialize data
            cache_data = await self._deserialize_ml_data(serialized_data)
            
            # Update hit count
            await self._update_hit_count(cache_key)
            
            self.stats["hits"] += 1
            logger.debug(f"Cache hit for features {cache_key}")
            
            return cache_data
            
        except Exception as e:
            logger.error(f"Error retrieving cached features: {e}")
            self.stats["misses"] += 1
            return None
    
    async def _store_cache_metadata(self, cache_entry: MLCacheEntry):
        """Store cache entry metadata."""
        cache_manager = await self._get_cache_manager()
        
        metadata_key = f"meta:{cache_entry.cache_key}"
        
        # Store as hash for easy field updates
        entry_dict = cache_entry.to_dict()
        for field, value in entry_dict.items():
            await cache_manager.hset(metadata_key, field, value, self.metadata_namespace)
        
        # Set expiry slightly longer than data TTL
        data_ttl = int((cache_entry.expires_at - datetime.utcnow()).total_seconds())
        metadata_ttl = data_ttl + 300  # 5 minutes buffer
        
        await cache_manager.expire(metadata_key, metadata_ttl, self.metadata_namespace)
    
    async def _update_hit_count(self, cache_key: str):
        """Update hit count for cache entry."""
        cache_manager = await self._get_cache_manager()
        
        metadata_key = f"meta:{cache_key}"
        await cache_manager.async_redis_client.hincrby(
            cache_manager._build_key(metadata_key, self.metadata_namespace),
            "hit_count", 1
        )
    
    async def invalidate_model_cache(self, model_id: str, 
                                   cache_types: Optional[List[MLCacheType]] = None) -> int:
        """Invalidate all cache entries for a model."""
        cache_manager = await self._get_cache_manager()
        
        patterns = []
        if cache_types:
            for cache_type in cache_types:
                patterns.append(f"{model_id}:{cache_type.value}:*")
        else:
            patterns.append(f"{model_id}:*")
        
        total_invalidated = 0
        for pattern in patterns:
            count = await cache_manager.invalidate_pattern(pattern, self.namespace)
            total_invalidated += count
            
            # Also invalidate metadata
            meta_pattern = f"meta:{pattern}"
            await cache_manager.invalidate_pattern(meta_pattern, self.metadata_namespace)
        
        # Publish invalidation event
        await publish_cache_invalidation(
            f"ml_model:{model_id}:*",
            self.namespace,
            pattern=True,
            source="ml_cache_manager"
        )
        
        self.stats["invalidations"] += total_invalidated
        logger.info(f"Invalidated {total_invalidated} cache entries for model {model_id}")
        
        return total_invalidated
    
    async def invalidate_feature_cache(self, feature_name: str) -> int:
        """Invalidate all cache entries for a feature."""
        cache_manager = await self._get_cache_manager()
        
        pattern = f"{feature_name}:feature_computation:*"
        count = await cache_manager.invalidate_pattern(pattern, self.namespace)
        
        # Also invalidate metadata
        meta_pattern = f"meta:{pattern}"
        await cache_manager.invalidate_pattern(meta_pattern, self.metadata_namespace)
        
        # Publish invalidation event
        await publish_cache_invalidation(
            f"ml_feature:{feature_name}:*",
            self.namespace,
            pattern=True,
            source="ml_cache_manager"
        )
        
        self.stats["invalidations"] += count
        logger.info(f"Invalidated {count} cache entries for feature {feature_name}")
        
        return count
    
    async def get_cache_statistics(self) -> Dict[str, Any]:
        """Get comprehensive cache statistics."""
        cache_manager = await self._get_cache_manager()
        
        stats = {
            "basic_stats": self.stats.copy(),
            "cache_types": {},
            "model_stats": {},
            "size_stats": {
                "total_entries": 0,
                "total_size_bytes": 0,
                "avg_size_bytes": 0
            }
        }
        
        try:
            # Scan metadata to get detailed stats
            meta_pattern = cache_manager._build_key("meta:*", self.metadata_namespace)
            
            if cache_manager.is_cluster:
                # For cluster, scan each node
                all_keys = []
                for node in cache_manager.async_redis_client.get_nodes():
                    keys = await node.keys(meta_pattern)
                    if keys:
                        all_keys.extend(keys)
            else:
                all_keys = await cache_manager.async_redis_client.keys(meta_pattern)
            
            if all_keys:
                # Get metadata for each entry
                for key in all_keys[:100]:  # Limit to first 100 for performance
                    try:
                        meta_data = await cache_manager.hgetall(
                            key.replace(cache_manager.key_prefix + self.metadata_namespace + ":", ""),
                            self.metadata_namespace
                        )
                        
                        if meta_data:
                            cache_entry = MLCacheEntry.from_dict(meta_data)
                            
                            # Count by type
                            cache_type = cache_entry.cache_type.value
                            if cache_type not in stats["cache_types"]:
                                stats["cache_types"][cache_type] = {"count": 0, "total_hits": 0}
                            
                            stats["cache_types"][cache_type]["count"] += 1
                            stats["cache_types"][cache_type]["total_hits"] += cache_entry.hit_count
                            
                            # Count by model
                            model_id = cache_entry.model_id
                            if model_id not in stats["model_stats"]:
                                stats["model_stats"][model_id] = {"entries": 0, "total_hits": 0}
                            
                            stats["model_stats"][model_id]["entries"] += 1
                            stats["model_stats"][model_id]["total_hits"] += cache_entry.hit_count
                            
                            # Size stats
                            stats["size_stats"]["total_entries"] += 1
                            stats["size_stats"]["total_size_bytes"] += cache_entry.size_bytes
                    
                    except Exception as e:
                        logger.error(f"Error processing metadata key {key}: {e}")
                
                # Calculate averages
                if stats["size_stats"]["total_entries"] > 0:
                    stats["size_stats"]["avg_size_bytes"] = (
                        stats["size_stats"]["total_size_bytes"] / 
                        stats["size_stats"]["total_entries"]
                    )
        
        except Exception as e:
            logger.error(f"Error getting cache statistics: {e}")
            stats["error"] = str(e)
        
        return stats
    
    async def cleanup_expired_entries(self) -> int:
        """Clean up expired cache entries and their metadata."""
        cache_manager = await self._get_cache_manager()
        
        try:
            # Get all metadata keys
            meta_pattern = cache_manager._build_key("meta:*", self.metadata_namespace)
            
            if cache_manager.is_cluster:
                # For cluster, scan each node
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
                    # Get metadata
                    meta_data = await cache_manager.hgetall(
                        key.replace(cache_manager.key_prefix + self.metadata_namespace + ":", ""),
                        self.metadata_namespace
                    )
                    
                    if meta_data:
                        cache_entry = MLCacheEntry.from_dict(meta_data)
                        
                        # Check if expired
                        if cache_entry.is_expired():
                            # Delete data and metadata
                            await cache_manager.delete(cache_entry.cache_key, self.namespace)
                            await cache_manager.delete(f"meta:{cache_entry.cache_key}", self.metadata_namespace)
                            cleaned_count += 1
                
                except Exception as e:
                    logger.error(f"Error cleaning cache entry {key}: {e}")
            
            logger.info(f"Cleaned up {cleaned_count} expired cache entries")
            return cleaned_count
        
        except Exception as e:
            logger.error(f"Error during cache cleanup: {e}")
            return 0


# Global ML cache manager instance
_ml_cache_manager: Optional[MLCacheManager] = None


async def get_ml_cache_manager() -> MLCacheManager:
    """Get or create global ML cache manager instance."""
    global _ml_cache_manager
    if _ml_cache_manager is None:
        _ml_cache_manager = MLCacheManager()
    return _ml_cache_manager


# Convenience decorators for ML caching
def cache_ml_prediction(model_id: str, ttl_factors: Optional[Dict[str, float]] = None):
    """Decorator for caching ML predictions."""
    def decorator(func):
        async def wrapper(*args, **kwargs):
            ml_cache = await get_ml_cache_manager()
            
            # Get input data from function arguments
            input_data = {"args": args, "kwargs": kwargs}
            
            # Try to get cached prediction
            cached_result = await ml_cache.get_cached_prediction(model_id, input_data)
            if cached_result:
                return cached_result["prediction"]
            
            # Call function and cache result
            if asyncio.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                result = func(*args, **kwargs)
            
            # Cache the result
            await ml_cache.cache_prediction(
                model_id, "1.0", input_data, result,
                ttl_factors=ttl_factors
            )
            
            return result
        
        return wrapper
    return decorator