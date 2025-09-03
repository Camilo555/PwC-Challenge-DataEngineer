"""
Query Result Caching for Expensive Database Operations

Advanced caching system for database queries with intelligent invalidation,
query analysis, and performance optimizations.
"""

import asyncio
import hashlib
import json
import pickle
import re
import time
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

import pandas as pd
from sqlalchemy import text

from core.caching.redis_cache_manager import RedisCacheManager, get_cache_manager
from core.caching.redis_streams import publish_cache_invalidation
from core.logging import get_logger

logger = get_logger(__name__)


class QueryType(Enum):
    """Types of database queries."""

    SELECT = "select"
    AGGREGATE = "aggregate"
    JOIN = "join"
    ANALYTICAL = "analytical"
    REPORT = "report"
    COUNT = "count"
    UNKNOWN = "unknown"


class InvalidationStrategy(Enum):
    """Query cache invalidation strategies."""

    TABLE_BASED = "table_based"
    TIME_BASED = "time_based"
    MANUAL = "manual"
    WRITE_THROUGH = "write_through"
    DEPENDENCY_BASED = "dependency_based"


@dataclass
class QueryCacheEntry:
    """Query cache entry metadata."""

    cache_key: str
    query_hash: str
    query_text: str
    query_type: QueryType
    tables_accessed: list[str]
    execution_time: float
    result_size: int
    row_count: int
    created_at: datetime
    expires_at: datetime
    hit_count: int = 0
    last_accessed: datetime = None
    parameters: dict[str, Any] = None
    tags: list[str] = None

    def __post_init__(self):
        if self.last_accessed is None:
            self.last_accessed = self.created_at
        if self.parameters is None:
            self.parameters = {}
        if self.tags is None:
            self.tags = []

    def is_expired(self) -> bool:
        """Check if cache entry is expired."""
        return datetime.utcnow() > self.expires_at


class QueryAnalyzer:
    """Analyzes SQL queries for caching decisions."""

    def __init__(self):
        # Regex patterns for query analysis
        self.select_pattern = re.compile(r"\bSELECT\b", re.IGNORECASE)
        self.from_pattern = re.compile(r"\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*)", re.IGNORECASE)
        self.join_pattern = re.compile(
            r"\b(?:INNER|LEFT|RIGHT|FULL|OUTER)?\s*JOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)", re.IGNORECASE
        )
        self.aggregate_pattern = re.compile(
            r"\b(?:COUNT|SUM|AVG|MIN|MAX|GROUP\s+BY)\b", re.IGNORECASE
        )
        self.where_pattern = re.compile(r"\bWHERE\b", re.IGNORECASE)
        self.order_pattern = re.compile(r"\bORDER\s+BY\b", re.IGNORECASE)

    def analyze_query(self, query: str) -> dict[str, Any]:
        """Analyze SQL query to extract metadata."""
        query_clean = self._clean_query(query)

        analysis = {
            "query_type": self._determine_query_type(query_clean),
            "tables": self._extract_tables(query_clean),
            "has_joins": bool(self.join_pattern.search(query_clean)),
            "has_aggregations": bool(self.aggregate_pattern.search(query_clean)),
            "has_where_clause": bool(self.where_pattern.search(query_clean)),
            "has_order_by": bool(self.order_pattern.search(query_clean)),
            "complexity_score": self._calculate_complexity(query_clean),
            "cache_score": 0.0,  # Will be calculated
        }

        # Calculate cache worthiness score
        analysis["cache_score"] = self._calculate_cache_score(analysis)

        return analysis

    def _clean_query(self, query: str) -> str:
        """Clean query for analysis."""
        # Remove extra whitespace and newlines
        cleaned = re.sub(r"\s+", " ", query.strip())
        # Remove comments
        cleaned = re.sub(r"--.*$", "", cleaned, flags=re.MULTILINE)
        cleaned = re.sub(r"/\*.*?\*/", "", cleaned, flags=re.DOTALL)
        return cleaned

    def _determine_query_type(self, query: str) -> QueryType:
        """Determine the type of query."""
        query_upper = query.upper()

        if "SELECT" not in query_upper:
            return QueryType.UNKNOWN

        if self.aggregate_pattern.search(query):
            return QueryType.AGGREGATE
        elif self.join_pattern.search(query):
            return QueryType.JOIN
        elif "COUNT(" in query_upper:
            return QueryType.COUNT
        elif any(word in query_upper for word in ["ANALYTICS", "WINDOW", "PARTITION"]):
            return QueryType.ANALYTICAL
        else:
            return QueryType.SELECT

    def _extract_tables(self, query: str) -> list[str]:
        """Extract table names from query."""
        tables = set()

        # Find tables in FROM clauses
        from_matches = self.from_pattern.findall(query)
        tables.update(from_matches)

        # Find tables in JOIN clauses
        join_matches = self.join_pattern.findall(query)
        tables.update(join_matches)

        return list(tables)

    def _calculate_complexity(self, query: str) -> float:
        """Calculate query complexity score."""
        score = 1.0

        # Base complexity factors
        if self.join_pattern.search(query):
            join_count = len(self.join_pattern.findall(query))
            score += join_count * 0.5

        if self.aggregate_pattern.search(query):
            score += 1.0

        if "SUBQUERY" in query.upper() or "(" in query:
            score += 0.5

        if self.order_pattern.search(query):
            score += 0.3

        # Length factor
        score += len(query) / 1000.0

        return min(score, 10.0)  # Cap at 10

    def _calculate_cache_score(self, analysis: dict[str, Any]) -> float:
        """Calculate how worthy a query is for caching."""
        score = 0.0

        # Base score by query type
        type_scores = {
            QueryType.AGGREGATE: 0.9,
            QueryType.ANALYTICAL: 0.8,
            QueryType.JOIN: 0.7,
            QueryType.SELECT: 0.5,
            QueryType.COUNT: 0.6,
            QueryType.UNKNOWN: 0.1,
        }

        score += type_scores.get(analysis["query_type"], 0.1)

        # Complexity bonus
        score += min(analysis["complexity_score"] * 0.1, 0.5)

        # Join bonus
        if analysis["has_joins"]:
            score += 0.2

        # Aggregation bonus
        if analysis["has_aggregations"]:
            score += 0.3

        return min(score, 1.0)


class QueryCacheManager:
    """Advanced query result caching manager."""

    def __init__(
        self,
        cache_manager: RedisCacheManager | None = None,
        default_ttl: int = 1800,  # 30 minutes
        min_execution_time: float = 0.1,  # Cache queries > 100ms
        min_cache_score: float = 0.5,
    ):
        self.cache_manager = cache_manager
        self.default_ttl = default_ttl
        self.min_execution_time = min_execution_time
        self.min_cache_score = min_cache_score
        self.namespace = "query_cache"
        self.metadata_namespace = "query_cache_meta"

        # Query analyzer
        self.analyzer = QueryAnalyzer()

        # Table invalidation mappings
        self.table_dependencies: dict[str, set[str]] = defaultdict(set)  # table -> cache_keys

        # TTL strategies per table
        self.table_ttls: dict[str, int] = {}

        # Statistics
        self.stats = {
            "hits": 0,
            "misses": 0,
            "executions": 0,
            "total_time_saved": 0.0,
            "cache_size": 0,
        }

    async def _get_cache_manager(self) -> RedisCacheManager:
        """Get cache manager instance."""
        if self.cache_manager is None:
            self.cache_manager = await get_cache_manager()
        return self.cache_manager

    def configure_table_ttl(self, table_name: str, ttl: int):
        """Configure TTL for queries involving specific tables."""
        self.table_ttls[table_name] = ttl
        logger.info(f"Configured TTL for table {table_name}: {ttl}s")

    def _build_query_key(self, query: str, parameters: dict[str, Any] = None) -> str:
        """Build cache key for query."""
        # Normalize query
        normalized_query = re.sub(r"\s+", " ", query.strip().upper())

        # Include parameters in key
        if parameters:
            param_str = json.dumps(parameters, sort_keys=True, default=str)
            key_input = f"{normalized_query}:{param_str}"
        else:
            key_input = normalized_query

        # Create hash
        query_hash = hashlib.md5(key_input.encode()).hexdigest()
        return f"query:{query_hash}"

    def _should_cache_query(
        self, query_analysis: dict[str, Any], execution_time: float, row_count: int
    ) -> bool:
        """Determine if query result should be cached."""
        # Check minimum execution time
        if execution_time < self.min_execution_time:
            return False

        # Check cache score
        if query_analysis["cache_score"] < self.min_cache_score:
            return False

        # Don't cache very large result sets (> 10k rows)
        if row_count > 10000:
            logger.warning(f"Result set too large to cache: {row_count} rows")
            return False

        return True

    def _calculate_ttl(self, query_analysis: dict[str, Any]) -> int:
        """Calculate TTL based on query and table characteristics."""
        ttl = self.default_ttl

        # Check table-specific TTLs
        min_table_ttl = float("inf")
        for table in query_analysis["tables"]:
            if table in self.table_ttls:
                min_table_ttl = min(min_table_ttl, self.table_ttls[table])

        if min_table_ttl != float("inf"):
            ttl = int(min_table_ttl)

        # Adjust based on query type
        type_multipliers = {
            QueryType.AGGREGATE: 1.5,
            QueryType.ANALYTICAL: 2.0,
            QueryType.COUNT: 1.2,
            QueryType.JOIN: 1.0,
            QueryType.SELECT: 0.8,
        }

        multiplier = type_multipliers.get(query_analysis["query_type"], 1.0)
        ttl = int(ttl * multiplier)

        # Ensure reasonable bounds
        return max(300, min(ttl, 7200))  # 5 minutes to 2 hours

    async def _serialize_result(self, result: Any) -> bytes:
        """Serialize query result for Redis storage."""
        try:
            if isinstance(result, pd.DataFrame):
                # Use pickle for DataFrames
                return pickle.dumps(result)
            elif hasattr(result, "fetchall"):
                # SQLAlchemy result
                rows = result.fetchall()
                columns = result.keys() if hasattr(result, "keys") else []
                serialized_result = {
                    "rows": [
                        dict(row._mapping) if hasattr(row, "_mapping") else dict(row)
                        for row in rows
                    ],
                    "columns": list(columns),
                }
                return pickle.dumps(serialized_result)
            else:
                # Generic serialization
                return pickle.dumps(result)
        except Exception as e:
            logger.error(f"Serialization error: {e}")
            raise

    async def _deserialize_result(self, data: bytes) -> Any:
        """Deserialize query result from Redis storage."""
        try:
            return pickle.loads(data)
        except Exception as e:
            logger.error(f"Deserialization error: {e}")
            raise

    async def cache_query_result(
        self,
        query: str,
        result: Any,
        execution_time: float,
        parameters: dict[str, Any] = None,
        tags: list[str] = None,
    ) -> bool:
        """Cache query result."""
        try:
            # Analyze query
            query_analysis = self.analyzer.analyze_query(query)

            # Count rows in result
            row_count = 0
            if isinstance(result, pd.DataFrame):
                row_count = len(result)
            elif hasattr(result, "rowcount"):
                row_count = result.rowcount or 0
            elif isinstance(result, list | tuple):
                row_count = len(result)

            # Check if should cache
            if not self._should_cache_query(query_analysis, execution_time, row_count):
                return False

            cache_manager = await self._get_cache_manager()

            # Build cache key
            cache_key = self._build_query_key(query, parameters)

            # Calculate TTL
            ttl = self._calculate_ttl(query_analysis)

            # Serialize result
            serialized_result = await self._serialize_result(result)
            result_size = len(serialized_result)

            # Prepare cache data
            cache_data = {
                "result": serialized_result,
                "query": query,
                "parameters": parameters or {},
                "execution_time": execution_time,
                "cached_at": datetime.utcnow().isoformat(),
                "query_analysis": query_analysis,
            }

            # Store in cache
            success = await cache_manager.set(cache_key, cache_data, ttl, self.namespace)

            if success:
                # Store metadata
                cache_entry = QueryCacheEntry(
                    cache_key=cache_key,
                    query_hash=hashlib.md5(query.encode()).hexdigest(),
                    query_text=query[:1000],  # Truncate for storage
                    query_type=query_analysis["query_type"],
                    tables_accessed=query_analysis["tables"],
                    execution_time=execution_time,
                    result_size=result_size,
                    row_count=row_count,
                    created_at=datetime.utcnow(),
                    expires_at=datetime.utcnow() + timedelta(seconds=ttl),
                    parameters=parameters or {},
                    tags=tags or [],
                )

                await self._store_cache_metadata(cache_entry)

                # Update table dependencies
                for table in query_analysis["tables"]:
                    self.table_dependencies[table].add(cache_key)

                self.stats["cache_size"] += 1
                logger.debug(f"Cached query result: {cache_key} (TTL: {ttl}s, Size: {result_size})")

            return success

        except Exception as e:
            logger.error(f"Error caching query result: {e}")
            return False

    async def get_cached_result(
        self, query: str, parameters: dict[str, Any] = None
    ) -> tuple[Any, bool]:
        """
        Get cached query result.

        Returns:
            Tuple of (result, was_cached)
        """
        try:
            cache_manager = await self._get_cache_manager()

            # Build cache key
            cache_key = self._build_query_key(query, parameters)

            # Get from cache
            cached_data = await cache_manager.get(cache_key, self.namespace)

            if cached_data is None:
                self.stats["misses"] += 1
                return None, False

            # Deserialize result
            result = await self._deserialize_result(cached_data["result"])

            # Update hit count and statistics
            await self._update_hit_count(cache_key)

            self.stats["hits"] += 1
            self.stats["total_time_saved"] += cached_data["execution_time"]

            logger.debug(f"Cache hit for query: {cache_key}")
            return result, True

        except Exception as e:
            logger.error(f"Error getting cached result: {e}")
            self.stats["misses"] += 1
            return None, False

    async def execute_with_cache(
        self, connection, query: str, parameters: dict[str, Any] = None, tags: list[str] = None
    ) -> Any:
        """Execute query with automatic caching."""
        # Try cache first
        result, was_cached = await self.get_cached_result(query, parameters)
        if was_cached:
            return result

        # Execute query
        start_time = time.time()

        try:
            if parameters:
                result = connection.execute(text(query), parameters)
            else:
                result = connection.execute(text(query))

            execution_time = time.time() - start_time
            self.stats["executions"] += 1

            # Cache result
            await self.cache_query_result(query, result, execution_time, parameters, tags)

            return result

        except Exception as e:
            logger.error(f"Query execution error: {e}")
            raise

    async def invalidate_table_cache(self, table_name: str) -> int:
        """Invalidate all cached queries involving a specific table."""
        if table_name not in self.table_dependencies:
            return 0

        cache_manager = await self._get_cache_manager()
        cache_keys = list(self.table_dependencies[table_name])

        invalidated_count = 0
        for cache_key in cache_keys:
            # Delete data
            success = await cache_manager.delete(cache_key, self.namespace)
            if success:
                # Delete metadata
                await cache_manager.delete(f"meta:{cache_key}", self.metadata_namespace)
                invalidated_count += 1

        # Clear dependencies
        self.table_dependencies[table_name].clear()

        # Publish invalidation event
        await publish_cache_invalidation(
            f"table:{table_name}:*", self.namespace, pattern=True, source="query_cache_manager"
        )

        self.stats["cache_size"] -= invalidated_count
        logger.info(f"Invalidated {invalidated_count} cached queries for table: {table_name}")

        return invalidated_count

    async def invalidate_by_pattern(self, query_pattern: str) -> int:
        """Invalidate cached queries matching pattern."""
        cache_manager = await self._get_cache_manager()

        # Use query text pattern matching
        pattern = f"query:*{query_pattern}*"
        count = await cache_manager.invalidate_pattern(pattern, self.namespace)

        # Also invalidate metadata
        meta_pattern = f"meta:{pattern}"
        await cache_manager.invalidate_pattern(meta_pattern, self.metadata_namespace)

        self.stats["cache_size"] -= count
        logger.info(f"Invalidated {count} cached queries matching pattern: {query_pattern}")

        return count

    async def _store_cache_metadata(self, cache_entry: QueryCacheEntry):
        """Store cache entry metadata."""
        cache_manager = await self._get_cache_manager()

        metadata_key = f"meta:{cache_entry.cache_key}"

        # Convert to dict for storage
        entry_dict = asdict(cache_entry)
        entry_dict["created_at"] = entry_dict["created_at"].isoformat()
        entry_dict["expires_at"] = entry_dict["expires_at"].isoformat()
        entry_dict["last_accessed"] = entry_dict["last_accessed"].isoformat()
        entry_dict["query_type"] = entry_dict["query_type"].value

        ttl = int((cache_entry.expires_at - datetime.utcnow()).total_seconds()) + 300

        await cache_manager.set(metadata_key, entry_dict, ttl, self.metadata_namespace)

    async def _update_hit_count(self, cache_key: str):
        """Update hit count for cache entry."""
        cache_manager = await self._get_cache_manager()

        metadata_key = f"meta:{cache_key}"
        current_metadata = await cache_manager.get(metadata_key, self.metadata_namespace)

        if current_metadata:
            current_metadata["hit_count"] = current_metadata.get("hit_count", 0) + 1
            current_metadata["last_accessed"] = datetime.utcnow().isoformat()

            await cache_manager.set(
                metadata_key, current_metadata, namespace=self.metadata_namespace
            )

    async def get_cache_statistics(self) -> dict[str, Any]:
        """Get comprehensive cache statistics."""
        cache_manager = await self._get_cache_manager()

        stats = {
            "basic_stats": self.stats.copy(),
            "query_types": defaultdict(lambda: {"count": 0, "avg_execution_time": 0.0}),
            "table_stats": {},
            "performance": {
                "hit_rate": 0.0,
                "avg_execution_time": 0.0,
                "total_time_saved": self.stats["total_time_saved"],
            },
        }

        try:
            # Calculate hit rate
            total_requests = self.stats["hits"] + self.stats["misses"]
            if total_requests > 0:
                stats["performance"]["hit_rate"] = self.stats["hits"] / total_requests

            # Get detailed stats from metadata
            meta_pattern = cache_manager._build_key("meta:query:*", self.metadata_namespace)

            if cache_manager.is_cluster:
                all_keys = []
                for node in cache_manager.async_redis_client.get_nodes():
                    keys = await node.keys(meta_pattern)
                    if keys:
                        all_keys.extend(keys)
            else:
                all_keys = await cache_manager.async_redis_client.keys(meta_pattern)

            total_execution_time = 0.0
            execution_count = 0

            for key in all_keys[:100]:  # Limit for performance
                try:
                    metadata = await cache_manager.get(
                        key.replace(cache_manager.key_prefix + self.metadata_namespace + ":", ""),
                        self.metadata_namespace,
                    )

                    if metadata:
                        query_type = metadata.get("query_type", "unknown")
                        execution_time = metadata.get("execution_time", 0.0)
                        tables = metadata.get("tables_accessed", [])

                        # Query type stats
                        stats["query_types"][query_type]["count"] += 1
                        stats["query_types"][query_type]["avg_execution_time"] += execution_time

                        # Table stats
                        for table in tables:
                            if table not in stats["table_stats"]:
                                stats["table_stats"][table] = {
                                    "cached_queries": 0,
                                    "dependencies": 0,
                                }
                            stats["table_stats"][table]["cached_queries"] += 1

                        total_execution_time += execution_time
                        execution_count += 1

                except Exception as e:
                    logger.error(f"Error processing metadata key {key}: {e}")

            # Calculate averages
            if execution_count > 0:
                stats["performance"]["avg_execution_time"] = total_execution_time / execution_count

            for query_type, type_stats in stats["query_types"].items():
                if type_stats["count"] > 0:
                    type_stats["avg_execution_time"] /= type_stats["count"]

            # Add table dependency counts
            for table, cache_keys in self.table_dependencies.items():
                if table in stats["table_stats"]:
                    stats["table_stats"][table]["dependencies"] = len(cache_keys)

        except Exception as e:
            logger.error(f"Error getting cache statistics: {e}")
            stats["error"] = str(e)

        return stats

    async def cleanup_expired_cache(self) -> int:
        """Clean up expired cache entries."""
        cache_manager = await self._get_cache_manager()

        try:
            meta_pattern = cache_manager._build_key("meta:query:*", self.metadata_namespace)

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
                        self.metadata_namespace,
                    )

                    if metadata:
                        expires_at = datetime.fromisoformat(metadata["expires_at"])

                        if datetime.utcnow() > expires_at:
                            cache_key = metadata["cache_key"]
                            tables = metadata.get("tables_accessed", [])

                            # Delete data and metadata
                            await cache_manager.delete(cache_key, self.namespace)
                            await cache_manager.delete(f"meta:{cache_key}", self.metadata_namespace)

                            # Clean up table dependencies
                            for table in tables:
                                if table in self.table_dependencies:
                                    self.table_dependencies[table].discard(cache_key)

                            cleaned_count += 1

                except Exception as e:
                    logger.error(f"Error cleaning cache entry {key}: {e}")

            self.stats["cache_size"] -= cleaned_count
            logger.info(f"Cleaned up {cleaned_count} expired query cache entries")
            return cleaned_count

        except Exception as e:
            logger.error(f"Error during query cache cleanup: {e}")
            return 0


# Global query cache manager instance
_query_cache_manager: QueryCacheManager | None = None


async def get_query_cache_manager() -> QueryCacheManager:
    """Get or create global query cache manager instance."""
    global _query_cache_manager
    if _query_cache_manager is None:
        _query_cache_manager = QueryCacheManager()
    return _query_cache_manager


# Decorator for automatic query caching
def cache_query(ttl: int | None = None, tags: list[str] | None = None):
    """Decorator for automatic query caching."""

    def decorator(func):
        async def wrapper(*args, **kwargs):
            query_cache = await get_query_cache_manager()

            # Execute function (should return query result)
            start_time = time.time()

            if asyncio.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                result = func(*args, **kwargs)

            execution_time = time.time() - start_time

            # Cache result if it's a query result
            if hasattr(result, "rowcount") or isinstance(result, pd.DataFrame | list):
                # Extract query from function or arguments
                query = kwargs.get("query") or (args[0] if args else "")
                parameters = kwargs.get("parameters") or kwargs.get("params")

                if query:
                    await query_cache.cache_query_result(
                        query, result, execution_time, parameters, tags
                    )

            return result

        return wrapper

    return decorator


# Context manager for automatic query caching
class CachedQueryContext:
    """Context manager for cached database queries."""

    def __init__(self, connection, query_cache: QueryCacheManager | None = None):
        self.connection = connection
        self.query_cache = query_cache

    async def __aenter__(self):
        if self.query_cache is None:
            self.query_cache = await get_query_cache_manager()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        pass

    async def execute(
        self, query: str, parameters: dict[str, Any] = None, tags: list[str] = None
    ) -> Any:
        """Execute query with caching."""
        return await self.query_cache.execute_with_cache(self.connection, query, parameters, tags)


# === ENHANCED CACHE WARMING AND INTELLIGENT INVALIDATION ===

class CacheWarmingStrategy:
    \"\"\"Advanced cache warming strategies for query performance optimization.\"\"\"
    
    def __init__(self, query_cache_manager: QueryCacheManager):
        self.cache_manager = query_cache_manager
        self.logger = logger
        
    async def warm_dashboard_queries(self, connection: Any) -> dict[str, Any]:
        \"\"\"Pre-warm cache with common dashboard queries.\"\"\"
        dashboard_queries = [
            # Sales performance queries
            {
                \"query\": \"\"\"
                    SELECT date_key, SUM(line_amount) as total_sales, COUNT(*) as transactions
                    FROM fact_sale 
                    WHERE date_key >= (SELECT MAX(date_key) - 30 FROM fact_sale)
                    GROUP BY date_key
                    ORDER BY date_key
                \"\"\",
                \"tags\": [\"dashboard\", \"sales_trend\", \"recent\"],
                \"priority\": 10
            },
            # Top products query
            {
                \"query\": \"\"\"
                    SELECT p.product_name, SUM(f.line_amount) as revenue, SUM(f.quantity) as units
                    FROM fact_sale f
                    JOIN dim_product p ON f.product_key = p.product_key
                    WHERE f.date_key >= (SELECT MAX(date_key) - 7 FROM fact_sale)
                    GROUP BY p.product_name
                    ORDER BY revenue DESC
                    LIMIT 20
                \"\"\",
                \"tags\": [\"dashboard\", \"top_products\", \"weekly\"],
                \"priority\": 9
            },
            # Customer analytics
            {
                \"query\": \"\"\"
                    SELECT c.customer_segment, COUNT(DISTINCT f.customer_key) as customers,
                           SUM(f.line_amount) as revenue
                    FROM fact_sale f
                    JOIN dim_customer c ON f.customer_key = c.customer_key
                    WHERE f.date_key >= (SELECT MAX(date_key) - 30 FROM fact_sale)
                    GROUP BY c.customer_segment
                    ORDER BY revenue DESC
                \"\"\",
                \"tags\": [\"dashboard\", \"customer_segments\", \"monthly\"],
                \"priority\": 8
            }
        ]
        
        results = {
            \"queries_warmed\": 0,
            \"cache_hits_generated\": 0,
            \"total_time\": 0,
            \"failed_queries\": []
        }
        
        start_time = time.time()
        
        for query_def in sorted(dashboard_queries, key=lambda x: x[\"priority\"], reverse=True):
            try:
                self.logger.info(f\"Warming cache for dashboard query: {query_def['tags']}\")
                
                # Execute query to populate cache
                await self.cache_manager.execute_with_cache(
                    connection, 
                    query_def[\"query\"], 
                    tags=query_def[\"tags\"]
                )
                
                results[\"queries_warmed\"] += 1
                
            except Exception as e:
                self.logger.warning(f\"Failed to warm cache for query {query_def['tags']}: {e}\")
                results[\"failed_queries\"].append({\"tags\": query_def[\"tags\"], \"error\": str(e)})
        
        results[\"total_time\"] = time.time() - start_time
        
        self.logger.info(f\"Cache warming completed: {results['queries_warmed']} queries warmed in {results['total_time']:.2f}s\")
        return results
    
    async def warm_analytical_queries(self, connection: Any, date_range_days: int = 30) -> dict[str, Any]:
        \"\"\"Pre-warm cache with common analytical queries.\"\"\"
        analytical_queries = [
            # Revenue trends
            {
                \"query\": f\"\"\"
                    SELECT 
                        date_key,
                        country_key,
                        SUM(line_amount) as revenue,
                        COUNT(*) as transactions,
                        AVG(line_amount) as avg_transaction
                    FROM fact_sale 
                    WHERE date_key >= (SELECT MAX(date_key) - {date_range_days} FROM fact_sale)
                    GROUP BY date_key, country_key
                    ORDER BY date_key, revenue DESC
                \"\"\",
                \"tags\": [\"analytics\", \"revenue_trends\", \"geographic\"],
            },
            # Product performance
            {
                \"query\": f\"\"\"
                    SELECT 
                        p.category,
                        p.subcategory,
                        SUM(f.line_amount) as revenue,
                        SUM(f.quantity) as units_sold,
                        COUNT(DISTINCT f.customer_key) as unique_customers
                    FROM fact_sale f
                    JOIN dim_product p ON f.product_key = p.product_key
                    WHERE f.date_key >= (SELECT MAX(date_key) - {date_range_days} FROM fact_sale)
                    GROUP BY p.category, p.subcategory
                    ORDER BY revenue DESC
                \"\"\",
                \"tags\": [\"analytics\", \"product_performance\", \"categories\"],
            }
        ]
        
        results = {\"queries_warmed\": 0, \"total_time\": 0, \"failed_queries\": []}
        start_time = time.time()
        
        for query_def in analytical_queries:
            try:
                await self.cache_manager.execute_with_cache(
                    connection, query_def[\"query\"], tags=query_def[\"tags\"]
                )
                results[\"queries_warmed\"] += 1
            except Exception as e:
                self.logger.warning(f\"Failed to warm analytical query: {e}\")
                results[\"failed_queries\"].append({\"tags\": query_def[\"tags\"], \"error\": str(e)})
        
        results[\"total_time\"] = time.time() - start_time
        return results


class IntelligentCacheInvalidation:
    \"\"\"Enhanced cache invalidation with dependency tracking and smart strategies.\"\"\"
    
    def __init__(self, query_cache_manager: QueryCacheManager):
        self.cache_manager = query_cache_manager
        self.logger = logger
        
        # Define table dependency relationships
        self.table_dependencies = {
            \"fact_sale\": [\"dim_product\", \"dim_customer\", \"dim_date\", \"dim_country\"],
            \"dim_product\": [\"fact_sale\"],
            \"dim_customer\": [\"fact_sale\"],
            \"dim_date\": [\"fact_sale\"],
            \"dim_country\": [\"fact_sale\"],
        }
        
    async def invalidate_with_cascade(self, table_name: str, cascade_depth: int = 2) -> dict[str, Any]:
        \"\"\"Invalidate cache with cascading dependencies.\"\"\"
        invalidated_tables = set()
        invalidation_count = 0
        
        # Track what we're invalidating
        tables_to_process = [(table_name, 0)]  # (table, depth)
        
        while tables_to_process and cascade_depth >= 0:
            current_table, current_depth = tables_to_process.pop(0)
            
            if current_table in invalidated_tables or current_depth > cascade_depth:
                continue
                
            # Invalidate current table
            count = await self.cache_manager.invalidate_table_cache(current_table)
            invalidated_tables.add(current_table)
            invalidation_count += count
            
            # Add dependent tables for processing
            dependencies = self.table_dependencies.get(current_table, [])
            for dep_table in dependencies:
                if dep_table not in invalidated_tables:
                    tables_to_process.append((dep_table, current_depth + 1))
        
        results = {
            \"invalidated_tables\": list(invalidated_tables),
            \"total_cache_entries\": invalidation_count,
            \"cascade_depth_used\": cascade_depth
        }
        
        self.logger.info(f\"Cascading invalidation completed: {len(invalidated_tables)} tables, {invalidation_count} cache entries\")
        return results
    
    async def smart_invalidate_by_transaction(self, transaction_tables: list[str]) -> dict[str, Any]:
        \"\"\"Intelligently invalidate cache based on database transaction scope.\"\"\"
        # Group related tables
        core_tables = set(transaction_tables)
        dependent_tables = set()
        
        # Find all dependent tables
        for table in transaction_tables:
            dependencies = self.table_dependencies.get(table, [])
            dependent_tables.update(dependencies)
        
        # Invalidate core tables immediately
        core_invalidation = 0
        for table in core_tables:
            count = await self.cache_manager.invalidate_table_cache(table)
            core_invalidation += count
        
        # Invalidate dependent tables with delay to allow for bulk operations
        await asyncio.sleep(0.1)  # Small delay for dependent invalidation
        
        dependent_invalidation = 0
        for table in dependent_tables - core_tables:  # Exclude already invalidated
            count = await self.cache_manager.invalidate_table_cache(table)
            dependent_invalidation += count
        
        return {
            \"core_tables_invalidated\": list(core_tables),
            \"dependent_tables_invalidated\": list(dependent_tables - core_tables),
            \"core_cache_entries\": core_invalidation,
            \"dependent_cache_entries\": dependent_invalidation,
            \"total_invalidated\": core_invalidation + dependent_invalidation
        }


# Enhanced QueryCacheManager extension methods
def add_enhanced_caching_to_query_manager(query_manager: QueryCacheManager):
    \"\"\"Add enhanced caching capabilities to existing QueryCacheManager.\"\"\"
    
    # Add cache warming
    query_manager._cache_warmer = CacheWarmingStrategy(query_manager)
    query_manager._intelligent_invalidation = IntelligentCacheInvalidation(query_manager)
    
    async def warm_dashboard_cache(self, connection: Any) -> dict[str, Any]:
        \"\"\"Warm cache with dashboard queries.\"\"\"
        return await self._cache_warmer.warm_dashboard_queries(connection)
    
    async def warm_analytical_cache(self, connection: Any, date_range_days: int = 30) -> dict[str, Any]:
        \"\"\"Warm cache with analytical queries.\"\"\"
        return await self._cache_warmer.warm_analytical_queries(connection, date_range_days)
    
    async def cascade_invalidate(self, table_name: str, cascade_depth: int = 2) -> dict[str, Any]:
        \"\"\"Invalidate with cascading dependencies.\"\"\"
        return await self._intelligent_invalidation.invalidate_with_cascade(table_name, cascade_depth)
    
    async def smart_transaction_invalidate(self, transaction_tables: list[str]) -> dict[str, Any]:
        \"\"\"Smart invalidation for transaction scope.\"\"\"
        return await self._intelligent_invalidation.smart_invalidate_by_transaction(transaction_tables)
    
    # Bind methods to instance
    import types
    query_manager.warm_dashboard_cache = types.MethodType(warm_dashboard_cache, query_manager)
    query_manager.warm_analytical_cache = types.MethodType(warm_analytical_cache, query_manager)
    query_manager.cascade_invalidate = types.MethodType(cascade_invalidate, query_manager)
    query_manager.smart_transaction_invalidate = types.MethodType(smart_transaction_invalidate, query_manager)
    
    return query_manager
