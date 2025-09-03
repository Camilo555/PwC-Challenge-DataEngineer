"""
GraphQL Redis Cache Integration
Implements intelligent caching strategies for GraphQL queries with Redis.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta
from typing import Any

import redis.asyncio as redis

from core.config.base_config import BaseConfig
from core.logging import get_logger

logger = get_logger(__name__)


class GraphQLRedisCache:
    """Redis-based caching for GraphQL queries with intelligent invalidation."""

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379/0",
        default_ttl: int = 300,
        key_prefix: str = "gql_cache",
    ):
        self.redis_url = redis_url
        self.default_ttl = default_ttl
        self.key_prefix = key_prefix
        self._redis_client: redis.Redis | None = None

        # Cache configuration by query type
        self.cache_config = {
            "sales": {"ttl": 300, "invalidation_tags": ["sales_data"]},
            "business_metrics": {"ttl": 600, "invalidation_tags": ["sales_data", "metrics"]},
            "customer_segments": {"ttl": 3600, "invalidation_tags": ["customer_data"]},
            "product_performance": {
                "ttl": 1800,
                "invalidation_tags": ["sales_data", "product_data"],
            },
            "sales_analytics": {"ttl": 900, "invalidation_tags": ["sales_data", "analytics"]},
        }

    async def get_redis_client(self) -> redis.Redis:
        """Get or create Redis client connection."""
        if self._redis_client is None:
            try:
                self._redis_client = redis.from_url(
                    self.redis_url,
                    encoding="utf-8",
                    decode_responses=True,
                    max_connections=20,
                    retry_on_timeout=True,
                    socket_keepalive=True,
                    socket_keepalive_options={},
                    health_check_interval=30,
                )
                # Test connection
                await self._redis_client.ping()
                logger.info("Redis client connected successfully")
            except Exception as e:
                logger.error(f"Failed to connect to Redis: {e}")
                # Create a mock client that always returns None for cache misses
                self._redis_client = MockRedisClient()

        return self._redis_client

    def generate_cache_key(
        self,
        query: str,
        variables: dict[str, Any] | None = None,
        user_context: dict[str, Any] | None = None,
    ) -> str:
        """Generate a deterministic cache key for the query."""
        # Include query, variables, and relevant user context
        key_data = {
            "query": query.strip(),
            "variables": variables or {},
            # Only include user context fields that affect query results
            "user_permissions": user_context.get("permissions", []) if user_context else [],
            "user_segment": user_context.get("customer_segment") if user_context else None,
        }

        # Create hash of the key data
        key_json = json.dumps(key_data, sort_keys=True, default=str)
        key_hash = hashlib.sha256(key_json.encode()).hexdigest()[:16]

        return f"{self.key_prefix}:{key_hash}"

    def extract_query_type(self, query: str) -> str | None:
        """Extract the main query type from GraphQL query string."""
        query_lower = query.lower().strip()

        # Map query patterns to types
        query_patterns = {
            "sales": ["sales(", "sales "],
            "business_metrics": ["business_metrics", "businessmetrics"],
            "customer_segments": ["customer_segments", "customersegments"],
            "product_performance": ["product_performance", "productperformance"],
            "sales_analytics": ["sales_analytics", "salesanalytics"],
        }

        for query_type, patterns in query_patterns.items():
            if any(pattern in query_lower for pattern in patterns):
                return query_type

        return None

    async def get(
        self,
        query: str,
        variables: dict[str, Any] | None = None,
        user_context: dict[str, Any] | None = None,
    ) -> dict[str, Any] | None:
        """Get cached result for a GraphQL query."""
        try:
            redis_client = await self.get_redis_client()
            cache_key = self.generate_cache_key(query, variables, user_context)

            # Try to get from cache
            cached_data = await redis_client.get(cache_key)
            if cached_data:
                result = json.loads(cached_data)

                # Check if cache entry has metadata
                if isinstance(result, dict) and "cached_at" in result:
                    cached_at = datetime.fromisoformat(result["cached_at"])
                    ttl = result.get("ttl", self.default_ttl)

                    # Check if cache is still valid
                    if datetime.utcnow() - cached_at < timedelta(seconds=ttl):
                        logger.debug(f"Cache hit for key: {cache_key}")
                        return result.get("data")
                    else:
                        # Cache expired, remove it
                        await redis_client.delete(cache_key)
                        logger.debug(f"Cache expired for key: {cache_key}")
                else:
                    # Legacy cache format, assume still valid
                    logger.debug(f"Cache hit (legacy) for key: {cache_key}")
                    return result

            logger.debug(f"Cache miss for key: {cache_key}")
            return None

        except Exception as e:
            logger.error(f"Error getting from cache: {e}")
            return None

    async def set(
        self,
        query: str,
        result: dict[str, Any],
        variables: dict[str, Any] | None = None,
        user_context: dict[str, Any] | None = None,
        ttl: int | None = None,
    ) -> bool:
        """Cache a GraphQL query result."""
        try:
            redis_client = await self.get_redis_client()
            cache_key = self.generate_cache_key(query, variables, user_context)

            # Determine TTL based on query type
            query_type = self.extract_query_type(query)
            if ttl is None:
                if query_type and query_type in self.cache_config:
                    ttl = self.cache_config[query_type]["ttl"]
                else:
                    ttl = self.default_ttl

            # Create cache entry with metadata
            cache_entry = {
                "data": result,
                "cached_at": datetime.utcnow().isoformat(),
                "ttl": ttl,
                "query_type": query_type,
            }

            # Store in Redis with TTL
            await redis_client.setex(cache_key, ttl, json.dumps(cache_entry, default=str))

            # Add to invalidation tags if query type is configured
            if query_type and query_type in self.cache_config:
                tags = self.cache_config[query_type]["invalidation_tags"]
                await self._add_to_invalidation_tags(cache_key, tags)

            logger.debug(f"Cached result for key: {cache_key}, TTL: {ttl}s")
            return True

        except Exception as e:
            logger.error(f"Error setting cache: {e}")
            return False

    async def _add_to_invalidation_tags(self, cache_key: str, tags: list[str]) -> None:
        """Add cache key to invalidation tag sets."""
        try:
            redis_client = await self.get_redis_client()

            for tag in tags:
                tag_key = f"{self.key_prefix}_tag:{tag}"
                await redis_client.sadd(tag_key, cache_key)
                # Set expiry on tag set (cleanup old tags)
                await redis_client.expire(tag_key, 7200)  # 2 hours

        except Exception as e:
            logger.error(f"Error adding to invalidation tags: {e}")

    async def invalidate_by_tags(self, tags: list[str]) -> int:
        """Invalidate all cache entries associated with given tags."""
        try:
            redis_client = await self.get_redis_client()
            invalidated_count = 0

            for tag in tags:
                tag_key = f"{self.key_prefix}_tag:{tag}"

                # Get all cache keys for this tag
                cache_keys = await redis_client.smembers(tag_key)

                if cache_keys:
                    # Delete all cache entries
                    deleted = await redis_client.delete(*cache_keys)
                    invalidated_count += deleted

                    # Remove the tag set
                    await redis_client.delete(tag_key)

                    logger.info(f"Invalidated {deleted} cache entries for tag: {tag}")

            return invalidated_count

        except Exception as e:
            logger.error(f"Error invalidating cache by tags: {e}")
            return 0

    async def invalidate_pattern(self, pattern: str) -> int:
        """Invalidate cache entries matching a pattern."""
        try:
            redis_client = await self.get_redis_client()

            # Find keys matching pattern
            keys = await redis_client.keys(f"{self.key_prefix}:{pattern}")

            if keys:
                deleted = await redis_client.delete(*keys)
                logger.info(f"Invalidated {deleted} cache entries matching pattern: {pattern}")
                return deleted

            return 0

        except Exception as e:
            logger.error(f"Error invalidating cache by pattern: {e}")
            return 0

    async def clear_all(self) -> int:
        """Clear all GraphQL cache entries."""
        try:
            redis_client = await self.get_redis_client()

            # Get all cache keys
            keys = await redis_client.keys(f"{self.key_prefix}:*")

            if keys:
                deleted = await redis_client.delete(*keys)
                logger.info(f"Cleared {deleted} cache entries")
                return deleted

            return 0

        except Exception as e:
            logger.error(f"Error clearing cache: {e}")
            return 0

    async def get_cache_stats(self) -> dict[str, Any]:
        """Get cache statistics."""
        try:
            redis_client = await self.get_redis_client()

            # Count cache entries
            cache_keys = await redis_client.keys(f"{self.key_prefix}:*")
            total_entries = len([k for k in cache_keys if not k.endswith("_tag")])

            # Count tag sets
            tag_keys = await redis_client.keys(f"{self.key_prefix}_tag:*")
            total_tags = len(tag_keys)

            # Get memory usage (approximate)
            await redis_client.memory_usage("dummy_key") if hasattr(
                redis_client, "memory_usage"
            ) else 0

            return {
                "total_entries": total_entries,
                "total_tags": total_tags,
                "redis_connected": True,
                "cache_prefix": self.key_prefix,
                "default_ttl": self.default_ttl,
            }

        except Exception as e:
            logger.error(f"Error getting cache stats: {e}")
            return {"total_entries": 0, "total_tags": 0, "redis_connected": False, "error": str(e)}


class MockRedisClient:
    """Mock Redis client for fallback when Redis is unavailable."""

    async def get(self, key: str) -> None:
        return None

    async def setex(self, key: str, ttl: int, value: str) -> bool:
        return True

    async def delete(self, *keys: str) -> int:
        return 0

    async def sadd(self, key: str, *values: str) -> int:
        return 0

    async def smembers(self, key: str) -> set:
        return set()

    async def keys(self, pattern: str) -> list[str]:
        return []

    async def expire(self, key: str, ttl: int) -> bool:
        return True

    async def ping(self) -> bool:
        return True


# Global cache instance
_cache_instance: GraphQLRedisCache | None = None


def get_graphql_cache() -> GraphQLRedisCache:
    """Get or create the global GraphQL cache instance."""
    global _cache_instance

    if _cache_instance is None:
        config = BaseConfig()
        redis_url = getattr(config, "redis_url", "redis://localhost:6379/0")
        _cache_instance = GraphQLRedisCache(redis_url=redis_url)

    return _cache_instance


async def invalidate_sales_cache():
    """Invalidate sales-related cache entries."""
    cache = get_graphql_cache()
    await cache.invalidate_by_tags(["sales_data"])


async def invalidate_customer_cache():
    """Invalidate customer-related cache entries."""
    cache = get_graphql_cache()
    await cache.invalidate_by_tags(["customer_data"])


async def invalidate_product_cache():
    """Invalidate product-related cache entries."""
    cache = get_graphql_cache()
    await cache.invalidate_by_tags(["product_data"])


async def invalidate_analytics_cache():
    """Invalidate analytics-related cache entries."""
    cache = get_graphql_cache()
    await cache.invalidate_by_tags(["analytics", "metrics"])
