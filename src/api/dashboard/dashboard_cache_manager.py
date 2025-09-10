"""
Dashboard Performance Cache Manager
Specialized Redis caching layer optimized for real-time business intelligence dashboards
"""
import json
import hashlib
import asyncio
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union
from dataclasses import dataclass, field
from enum import Enum

import redis.asyncio as aioredis
from redis.asyncio import ConnectionPool

from src.core.caching.redis_cache_manager import RedisCacheManager
from src.streaming.kafka_manager import create_kafka_manager, StreamingTopic
from src.etl.gold.materialized_views_manager import create_materialized_views_manager
from core.config.unified_config import get_unified_config
from core.logging import get_logger


class CacheStrategy(Enum):
    """Dashboard cache strategies"""
    WRITE_THROUGH = "write_through"    # Write to cache and database simultaneously
    WRITE_BEHIND = "write_behind"      # Write to cache first, database later
    CACHE_ASIDE = "cache_aside"        # Application manages cache
    REFRESH_AHEAD = "refresh_ahead"    # Proactive refresh before expiration


class DashboardCacheLevel(Enum):
    """Multi-level cache hierarchy"""
    L1_MEMORY = "l1_memory"           # In-memory application cache
    L2_REDIS = "l2_redis"             # Redis distributed cache
    L3_MATERIALIZED = "l3_materialized" # Materialized views
    L4_DATABASE = "l4_database"       # Source database


@dataclass
class CacheMetrics:
    """Cache performance metrics"""
    hits: int = 0
    misses: int = 0
    evictions: int = 0
    refresh_count: int = 0
    total_requests: int = 0
    avg_response_time_ms: float = 0.0
    cache_size_mb: float = 0.0
    hit_rate_percentage: float = 0.0


@dataclass
class DashboardCacheConfig:
    """Configuration for dashboard cache"""
    cache_name: str
    ttl_seconds: int = 300  # 5 minutes default
    max_size_mb: int = 100
    strategy: CacheStrategy = CacheStrategy.CACHE_ASIDE
    enable_compression: bool = True
    enable_encryption: bool = False
    auto_refresh: bool = True
    refresh_threshold: float = 0.8  # Refresh when 80% of TTL elapsed
    priority: int = 1  # 1 = highest, 5 = lowest


class DashboardCacheManager:
    """
    Specialized cache manager for business intelligence dashboards
    Optimized for real-time KPI delivery with multi-level caching
    """

    def __init__(self):
        self.config = get_unified_config()
        self.logger = get_logger(__name__)
        
        # Initialize Redis cache manager
        self.redis_cache = RedisCacheManager()
        
        # Initialize Kafka for cache invalidation events
        self.kafka_manager = create_kafka_manager()
        
        # Initialize materialized views manager
        self.views_manager = create_materialized_views_manager()
        
        # Dashboard cache configurations
        self.cache_configs = self._initialize_cache_configs()
        
        # In-memory L1 cache for ultra-fast access
        self.l1_cache: Dict[str, Dict[str, Any]] = {}
        
        # Cache metrics tracking
        self.metrics: Dict[str, CacheMetrics] = {
            name: CacheMetrics() for name in self.cache_configs.keys()
        }
        
        # Auto-refresh tasks
        self.refresh_tasks: Dict[str, asyncio.Task] = {}

    def _initialize_cache_configs(self) -> Dict[str, DashboardCacheConfig]:
        """Initialize dashboard cache configurations"""
        return {
            # Real-time Executive KPIs - Highest priority, shortest TTL
            "executive_kpis": DashboardCacheConfig(
                cache_name="executive_kpis",
                ttl_seconds=60,  # 1 minute
                max_size_mb=50,
                strategy=CacheStrategy.REFRESH_AHEAD,
                auto_refresh=True,
                refresh_threshold=0.7,  # Refresh at 70% TTL
                priority=1
            ),
            
            # Revenue Analytics - High priority, moderate TTL
            "revenue_analytics": DashboardCacheConfig(
                cache_name="revenue_analytics",
                ttl_seconds=300,  # 5 minutes
                max_size_mb=100,
                strategy=CacheStrategy.WRITE_THROUGH,
                auto_refresh=True,
                refresh_threshold=0.8,
                priority=1
            ),
            
            # Customer Behavior - Medium priority, longer TTL
            "customer_behavior": DashboardCacheConfig(
                cache_name="customer_behavior",
                ttl_seconds=900,  # 15 minutes
                max_size_mb=150,
                strategy=CacheStrategy.CACHE_ASIDE,
                auto_refresh=True,
                refresh_threshold=0.85,
                priority=2
            ),
            
            # Operational Metrics - High priority for ops team
            "operational_metrics": DashboardCacheConfig(
                cache_name="operational_metrics",
                ttl_seconds=120,  # 2 minutes
                max_size_mb=75,
                strategy=CacheStrategy.REFRESH_AHEAD,
                auto_refresh=True,
                refresh_threshold=0.75,
                priority=1
            ),
            
            # Data Quality Dashboard - Medium priority
            "data_quality": DashboardCacheConfig(
                cache_name="data_quality",
                ttl_seconds=600,  # 10 minutes
                max_size_mb=50,
                strategy=CacheStrategy.CACHE_ASIDE,
                auto_refresh=True,
                priority=2
            ),
            
            # API Performance Metrics - Critical for monitoring
            "api_performance": DashboardCacheConfig(
                cache_name="api_performance",
                ttl_seconds=30,  # 30 seconds
                max_size_mb=25,
                strategy=CacheStrategy.WRITE_THROUGH,
                auto_refresh=True,
                refresh_threshold=0.6,  # Very aggressive refresh
                priority=1
            ),
            
            # Financial KPIs - Business critical
            "financial_kpis": DashboardCacheConfig(
                cache_name="financial_kpis",
                ttl_seconds=180,  # 3 minutes
                max_size_mb=100,
                strategy=CacheStrategy.REFRESH_AHEAD,
                auto_refresh=True,
                refresh_threshold=0.8,
                priority=1
            ),
            
            # User Session Data - For personalized dashboards
            "user_sessions": DashboardCacheConfig(
                cache_name="user_sessions",
                ttl_seconds=1800,  # 30 minutes
                max_size_mb=200,
                strategy=CacheStrategy.WRITE_BEHIND,
                auto_refresh=False,  # User-specific, no auto-refresh
                priority=3
            )
        }

    async def get_dashboard_data(
        self,
        cache_name: str,
        key: str,
        user_id: Optional[str] = None,
        filters: Optional[Dict[str, Any]] = None,
        force_refresh: bool = False
    ) -> Optional[Dict[str, Any]]:
        """
        Get dashboard data with multi-level caching strategy
        
        Args:
            cache_name: Name of cache configuration
            key: Cache key
            user_id: Optional user ID for personalization
            filters: Optional filters for data
            force_refresh: Force refresh from source
            
        Returns:
            Cached data or None if not found
        """
        config = self.cache_configs.get(cache_name)
        if not config:
            self.logger.error(f"Cache config not found: {cache_name}")
            return None

        # Generate composite key
        composite_key = self._generate_cache_key(cache_name, key, user_id, filters)
        
        start_time = datetime.now()
        
        try:
            # Level 1: Check in-memory cache first
            if not force_refresh and composite_key in self.l1_cache:
                cached_data = self.l1_cache[composite_key]
                if self._is_cache_valid(cached_data):
                    self.metrics[cache_name].hits += 1
                    self.metrics[cache_name].total_requests += 1
                    self._update_response_time(cache_name, start_time)
                    
                    self.logger.debug(f"L1 cache hit for {composite_key}")
                    return cached_data["data"]
                else:
                    # Remove expired data
                    del self.l1_cache[composite_key]

            # Level 2: Check Redis cache
            if not force_refresh:
                redis_data = await self.redis_cache.get(composite_key)
                if redis_data:
                    # Store in L1 cache
                    self.l1_cache[composite_key] = {
                        "data": redis_data,
                        "timestamp": datetime.now(),
                        "ttl": config.ttl_seconds
                    }
                    
                    self.metrics[cache_name].hits += 1
                    self.metrics[cache_name].total_requests += 1
                    self._update_response_time(cache_name, start_time)
                    
                    self.logger.debug(f"L2 cache hit for {composite_key}")
                    return redis_data

            # Level 3: Check materialized views
            view_data = await self._get_from_materialized_view(cache_name, key, filters)
            if view_data:
                # Cache in both levels
                await self._store_in_cache(cache_name, composite_key, view_data, config)
                
                self.metrics[cache_name].misses += 1
                self.metrics[cache_name].total_requests += 1
                self._update_response_time(cache_name, start_time)
                
                self.logger.debug(f"L3 materialized view hit for {composite_key}")
                return view_data

            # Level 4: Fallback to database
            db_data = await self._get_from_database(cache_name, key, filters)
            if db_data:
                # Cache in all levels
                await self._store_in_cache(cache_name, composite_key, db_data, config)
                
                self.metrics[cache_name].misses += 1
                self.metrics[cache_name].total_requests += 1
                self._update_response_time(cache_name, start_time)
                
                self.logger.debug(f"L4 database fallback for {composite_key}")
                return db_data

            # Data not found anywhere
            self.metrics[cache_name].misses += 1
            self.metrics[cache_name].total_requests += 1
            
            return None

        except Exception as e:
            self.metrics[cache_name].total_requests += 1
            self.logger.error(f"Error getting dashboard data for {composite_key}: {e}")
            return None

    async def _store_in_cache(
        self,
        cache_name: str,
        composite_key: str,
        data: Dict[str, Any],
        config: DashboardCacheConfig
    ):
        """Store data in multi-level cache"""
        try:
            # Store in L1 cache
            self.l1_cache[composite_key] = {
                "data": data,
                "timestamp": datetime.now(),
                "ttl": config.ttl_seconds
            }
            
            # Store in L2 Redis cache
            await self.redis_cache.set(
                composite_key,
                data,
                ttl=config.ttl_seconds
            )
            
            # Manage L1 cache size
            await self._manage_l1_cache_size()
            
            # Schedule auto-refresh if enabled
            if config.auto_refresh:
                await self._schedule_auto_refresh(cache_name, composite_key, config)
                
        except Exception as e:
            self.logger.error(f"Error storing in cache: {e}")

    async def _get_from_materialized_view(
        self,
        cache_name: str,
        key: str,
        filters: Optional[Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        """Get data from materialized views"""
        try:
            # Map cache names to materialized view names
            view_mapping = {
                "executive_kpis": "executive_revenue_kpis",
                "revenue_analytics": "realtime_sales_metrics",
                "customer_behavior": "customer_behavior_metrics",
                "operational_metrics": "executive_operational_kpis",
                "data_quality": "data_quality_dashboard",
                "api_performance": "api_performance_metrics",
                "financial_kpis": "financial_kpis_hourly"
            }
            
            view_name = view_mapping.get(cache_name)
            if not view_name:
                return None
                
            # This would be implemented based on your specific view query logic
            # For now, return placeholder
            return {
                "source": "materialized_view",
                "view_name": view_name,
                "timestamp": datetime.now().isoformat(),
                "filters_applied": filters or {}
            }
            
        except Exception as e:
            self.logger.error(f"Error getting from materialized view: {e}")
            return None

    async def _get_from_database(
        self,
        cache_name: str,
        key: str,
        filters: Optional[Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        """Get data from database as fallback"""
        try:
            # This would be implemented with actual database queries
            # For now, return placeholder indicating database fallback
            return {
                "source": "database",
                "cache_name": cache_name,
                "key": key,
                "timestamp": datetime.now().isoformat(),
                "filters_applied": filters or {},
                "warning": "Fallback to database - consider refreshing materialized views"
            }
            
        except Exception as e:
            self.logger.error(f"Error getting from database: {e}")
            return None

    def _generate_cache_key(
        self,
        cache_name: str,
        key: str,
        user_id: Optional[str],
        filters: Optional[Dict[str, Any]]
    ) -> str:
        """Generate composite cache key"""
        key_parts = [cache_name, key]
        
        if user_id:
            key_parts.append(f"user:{user_id}")
            
        if filters:
            # Sort filters for consistent key generation
            sorted_filters = json.dumps(filters, sort_keys=True)
            filter_hash = hashlib.md5(sorted_filters.encode()).hexdigest()
            key_parts.append(f"filters:{filter_hash}")
            
        return ":".join(key_parts)

    def _is_cache_valid(self, cached_data: Dict[str, Any]) -> bool:
        """Check if cached data is still valid"""
        if "timestamp" not in cached_data or "ttl" not in cached_data:
            return False
            
        timestamp = cached_data["timestamp"]
        ttl = cached_data["ttl"]
        
        if isinstance(timestamp, str):
            timestamp = datetime.fromisoformat(timestamp)
            
        age_seconds = (datetime.now() - timestamp).total_seconds()
        return age_seconds < ttl

    async def _manage_l1_cache_size(self):
        """Manage L1 cache size by evicting old entries"""
        # Simple LRU-like eviction - could be enhanced
        if len(self.l1_cache) > 1000:  # Max 1000 entries in L1
            # Remove oldest 20% of entries
            sorted_items = sorted(
                self.l1_cache.items(),
                key=lambda x: x[1].get("timestamp", datetime.min)
            )
            
            for key, _ in sorted_items[:200]:
                del self.l1_cache[key]

    async def _schedule_auto_refresh(
        self,
        cache_name: str,
        composite_key: str,
        config: DashboardCacheConfig
    ):
        """Schedule automatic cache refresh"""
        if composite_key in self.refresh_tasks:
            # Cancel existing refresh task
            self.refresh_tasks[composite_key].cancel()
            
        # Calculate refresh time
        refresh_delay = config.ttl_seconds * config.refresh_threshold
        
        async def refresh_task():
            await asyncio.sleep(refresh_delay)
            try:
                # Refresh the cache entry
                await self.refresh_cache_entry(cache_name, composite_key)
                self.metrics[cache_name].refresh_count += 1
            except Exception as e:
                self.logger.error(f"Error in auto-refresh task: {e}")
            finally:
                # Remove from refresh tasks
                self.refresh_tasks.pop(composite_key, None)
                
        # Schedule the refresh task
        self.refresh_tasks[composite_key] = asyncio.create_task(refresh_task())

    async def refresh_cache_entry(self, cache_name: str, composite_key: str):
        """Refresh a specific cache entry"""
        try:
            # Extract original parameters from composite key
            parts = composite_key.split(":")
            key = parts[1] if len(parts) > 1 else composite_key
            
            # Force refresh from source
            fresh_data = await self.get_dashboard_data(
                cache_name=cache_name,
                key=key,
                force_refresh=True
            )
            
            if fresh_data:
                self.logger.debug(f"Refreshed cache entry: {composite_key}")
            else:
                self.logger.warning(f"Failed to refresh cache entry: {composite_key}")
                
        except Exception as e:
            self.logger.error(f"Error refreshing cache entry {composite_key}: {e}")

    async def invalidate_cache(
        self,
        cache_name: str,
        key: Optional[str] = None,
        pattern: Optional[str] = None
    ):
        """Invalidate cache entries"""
        try:
            if key:
                # Invalidate specific key
                composite_key = f"{cache_name}:{key}"
                
                # Remove from L1 cache
                keys_to_remove = [k for k in self.l1_cache.keys() if k.startswith(composite_key)]
                for k in keys_to_remove:
                    del self.l1_cache[k]
                    
                # Remove from Redis
                await self.redis_cache.delete(composite_key)
                
            elif pattern:
                # Invalidate by pattern
                pattern_key = f"{cache_name}:{pattern}"
                
                # Remove from L1 cache
                keys_to_remove = [k for k in self.l1_cache.keys() if pattern_key in k]
                for k in keys_to_remove:
                    del self.l1_cache[k]
                    
                # Remove from Redis using pattern
                await self.redis_cache.delete_pattern(pattern_key)
                
            else:
                # Invalidate entire cache
                keys_to_remove = [k for k in self.l1_cache.keys() if k.startswith(cache_name)]
                for k in keys_to_remove:
                    del self.l1_cache[k]
                    
                # Remove from Redis
                await self.redis_cache.delete_pattern(f"{cache_name}:*")
                
            # Publish cache invalidation event
            await self._publish_cache_event("invalidate", cache_name, key or pattern or "*")
            
        except Exception as e:
            self.logger.error(f"Error invalidating cache: {e}")

    async def _publish_cache_event(self, operation: str, cache_name: str, key: str):
        """Publish cache events to Kafka for monitoring"""
        try:
            event_data = {
                "operation": operation,
                "cache_name": cache_name,
                "key": key,
                "timestamp": datetime.now().isoformat(),
                "source": "dashboard_cache_manager"
            }
            
            success = self.kafka_manager.produce_message(
                topic=StreamingTopic.CACHE_EVENTS,
                message=event_data,
                key=f"{cache_name}:{key}"
            )
            
            if success:
                self.logger.debug(f"Published cache event: {operation} for {cache_name}:{key}")
                
        except Exception as e:
            self.logger.error(f"Error publishing cache event: {e}")

    def _update_response_time(self, cache_name: str, start_time: datetime):
        """Update average response time metric"""
        duration_ms = (datetime.now() - start_time).total_seconds() * 1000
        current_avg = self.metrics[cache_name].avg_response_time_ms
        total_requests = self.metrics[cache_name].total_requests
        
        # Calculate running average
        if total_requests > 1:
            self.metrics[cache_name].avg_response_time_ms = (
                (current_avg * (total_requests - 1) + duration_ms) / total_requests
            )
        else:
            self.metrics[cache_name].avg_response_time_ms = duration_ms

    async def get_cache_metrics(self) -> Dict[str, Any]:
        """Get comprehensive cache metrics"""
        overall_metrics = {
            "total_caches": len(self.cache_configs),
            "l1_cache_entries": len(self.l1_cache),
            "total_requests": sum(m.total_requests for m in self.metrics.values()),
            "total_hits": sum(m.hits for m in self.metrics.values()),
            "total_misses": sum(m.misses for m in self.metrics.values()),
            "overall_hit_rate": 0.0,
            "active_refresh_tasks": len(self.refresh_tasks),
            "timestamp": datetime.now().isoformat()
        }
        
        # Calculate overall hit rate
        total_ops = overall_metrics["total_hits"] + overall_metrics["total_misses"]
        if total_ops > 0:
            overall_metrics["overall_hit_rate"] = (overall_metrics["total_hits"] / total_ops) * 100
            
        # Individual cache metrics
        cache_metrics = {}
        for cache_name, metrics in self.metrics.items():
            total_ops = metrics.hits + metrics.misses
            hit_rate = (metrics.hits / total_ops * 100) if total_ops > 0 else 0.0
            
            cache_metrics[cache_name] = {
                "hits": metrics.hits,
                "misses": metrics.misses,
                "total_requests": metrics.total_requests,
                "hit_rate_percentage": round(hit_rate, 2),
                "avg_response_time_ms": round(metrics.avg_response_time_ms, 2),
                "refresh_count": metrics.refresh_count,
                "config": {
                    "ttl_seconds": self.cache_configs[cache_name].ttl_seconds,
                    "strategy": self.cache_configs[cache_name].strategy.value,
                    "auto_refresh": self.cache_configs[cache_name].auto_refresh,
                    "priority": self.cache_configs[cache_name].priority
                }
            }
            
        return {
            "overall": overall_metrics,
            "by_cache": cache_metrics
        }

    async def warm_up_cache(self, cache_names: Optional[List[str]] = None):
        """Warm up cache with frequently accessed data"""
        caches_to_warm = cache_names or list(self.cache_configs.keys())
        
        self.logger.info(f"Warming up caches: {caches_to_warm}")
        
        # Common warm-up keys for each cache
        warmup_keys = {
            "executive_kpis": ["hourly_revenue", "daily_revenue", "customer_count"],
            "revenue_analytics": ["sales_by_hour", "top_products", "revenue_trends"],
            "customer_behavior": ["customer_segments", "retention_analysis"],
            "operational_metrics": ["system_health", "pipeline_status"],
            "data_quality": ["quality_scores", "validation_results"],
            "api_performance": ["endpoint_metrics", "response_times"],
            "financial_kpis": ["profit_margins", "cost_analysis"]
        }
        
        for cache_name in caches_to_warm:
            keys = warmup_keys.get(cache_name, [])
            for key in keys:
                try:
                    await self.get_dashboard_data(cache_name, key)
                    self.logger.debug(f"Warmed up {cache_name}:{key}")
                except Exception as e:
                    self.logger.error(f"Error warming up {cache_name}:{key}: {e}")

    async def close(self):
        """Close cache manager and cleanup resources"""
        try:
            # Cancel all refresh tasks
            for task in self.refresh_tasks.values():
                task.cancel()
                
            # Clear L1 cache
            self.l1_cache.clear()
            
            # Close Redis connections
            await self.redis_cache.close()
            
            # Close Kafka connections
            self.kafka_manager.close()
            
            # Close materialized views manager
            await self.views_manager.close()
            
            self.logger.info("Dashboard cache manager closed successfully")
            
        except Exception as e:
            self.logger.error(f"Error closing dashboard cache manager: {e}")


# Factory function
def create_dashboard_cache_manager() -> DashboardCacheManager:
    """Create DashboardCacheManager instance"""
    return DashboardCacheManager()


# Example usage
async def main():
    """Example usage of dashboard cache manager"""
    cache_manager = create_dashboard_cache_manager()
    
    try:
        # Warm up the cache
        await cache_manager.warm_up_cache()
        
        # Test getting executive KPIs
        kpi_data = await cache_manager.get_dashboard_data(
            cache_name="executive_kpis",
            key="hourly_revenue",
            user_id="exec_001",
            filters={"timeframe": "last_24h"}
        )
        
        if kpi_data:
            print(f"✅ Retrieved KPI data: {kpi_data}")
            
        # Test cache metrics
        metrics = await cache_manager.get_cache_metrics()
        print(f"📊 Cache Metrics:")
        print(f"Overall hit rate: {metrics['overall']['overall_hit_rate']:.1f}%")
        print(f"L1 cache entries: {metrics['overall']['l1_cache_entries']}")
        print(f"Active refresh tasks: {metrics['overall']['active_refresh_tasks']}")
        
        # Test cache invalidation
        await cache_manager.invalidate_cache("executive_kpis", "hourly_revenue")
        print("✅ Cache invalidation completed")
        
    finally:
        await cache_manager.close()


if __name__ == "__main__":
    asyncio.run(main())