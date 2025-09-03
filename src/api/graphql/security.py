"""
GraphQL Security Extensions
Implements query complexity analysis, depth limiting, and rate limiting for GraphQL.
"""

from __future__ import annotations

import time
from collections import defaultdict
from typing import Any

from graphql import GraphQLError
from strawberry.extensions import Extension

from core.logging import get_logger

logger = get_logger(__name__)


class QueryComplexityExtension(Extension):
    """Extension for analyzing and limiting GraphQL query complexity."""

    def __init__(
        self, max_complexity: int = 1000, max_depth: int = 10, introspection_enabled: bool = True
    ):
        self.max_complexity = max_complexity
        self.max_depth = max_depth
        self.introspection_enabled = introspection_enabled

    def on_validation_start(self) -> None:
        """Called before query validation."""
        if not self.execution_context.query:
            return

        # Analyze query complexity and depth
        try:
            complexity_analysis = self._analyze_query_complexity()
            depth_analysis = self._analyze_query_depth()

            # Check complexity limit
            if complexity_analysis["total_complexity"] > self.max_complexity:
                raise GraphQLError(
                    f"Query complexity {complexity_analysis['total_complexity']} "
                    f"exceeds maximum allowed complexity {self.max_complexity}."
                )

            # Check depth limit
            if depth_analysis["max_depth"] > self.max_depth:
                raise GraphQLError(
                    f"Query depth {depth_analysis['max_depth']} "
                    f"exceeds maximum allowed depth {self.max_depth}."
                )

            # Check for introspection queries in production
            if not self.introspection_enabled and depth_analysis["is_introspection"]:
                raise GraphQLError("Introspection is disabled.")

            # Store analysis results for logging
            self.execution_context.context["query_analysis"] = {
                "complexity": complexity_analysis,
                "depth": depth_analysis,
            }

        except Exception as e:
            logger.error(f"Error during query analysis: {e}")
            if isinstance(e, GraphQLError):
                raise
            raise GraphQLError("Query analysis failed.")

    def _analyze_query_complexity(self) -> dict[str, Any]:
        """Analyze query complexity using a simple scoring system."""
        query_document = self.execution_context.query
        if not query_document:
            return {"total_complexity": 0, "field_scores": {}}

        # Simple complexity scoring based on field depth and connections
        field_scores = {}
        total_complexity = 0

        # This is a simplified implementation
        # In production, you'd use a more sophisticated AST visitor
        query_str = str(query_document)

        # Basic scoring rules
        complexity_rules = {
            "sales": 10,  # Base sales query
            "customer": 5,  # Customer lookup
            "product": 5,  # Product lookup
            "country": 3,  # Country lookup
            "analytics": 20,  # Analytics queries are more expensive
            "segments": 15,  # Segmentation analysis
            "performance": 15,  # Performance metrics
            "business_metrics": 25,  # Complex business metrics
        }

        # Count field occurrences and calculate complexity
        for field, base_score in complexity_rules.items():
            occurrences = query_str.lower().count(field.lower())
            if occurrences > 0:
                field_complexity = base_score * occurrences
                field_scores[field] = field_complexity
                total_complexity += field_complexity

        # Add pagination multiplier
        if "page_size" in query_str.lower():
            # Extract page_size value if possible (simplified)
            import re

            page_size_match = re.search(r"page_size:\s*(\d+)", query_str.lower())
            if page_size_match:
                page_size = int(page_size_match.group(1))
                # Add complexity based on page size
                pagination_complexity = min(page_size // 10, 50)  # Cap at 50
                total_complexity += pagination_complexity
                field_scores["pagination"] = pagination_complexity

        return {"total_complexity": total_complexity, "field_scores": field_scores}

    def _analyze_query_depth(self) -> dict[str, Any]:
        """Analyze query depth."""
        query_document = self.execution_context.query
        if not query_document:
            return {"max_depth": 0, "is_introspection": False}

        query_str = str(query_document)

        # Simple depth calculation by counting nested braces
        max_depth = 0
        current_depth = 0

        for char in query_str:
            if char == "{":
                current_depth += 1
                max_depth = max(max_depth, current_depth)
            elif char == "}":
                current_depth -= 1

        # Check for introspection
        is_introspection = "__schema" in query_str or "__type" in query_str

        return {"max_depth": max_depth, "is_introspection": is_introspection}


class QueryTimeoutExtension(Extension):
    """Extension for timing out long-running queries."""

    def __init__(self, timeout_seconds: float = 30.0):
        self.timeout_seconds = timeout_seconds
        self.start_time = None

    def on_execute(self) -> None:
        """Called when query execution starts."""
        self.start_time = time.time()

    def on_end(self) -> None:
        """Called when query execution ends."""
        if self.start_time:
            execution_time = time.time() - self.start_time

            # Store execution time in context
            if self.execution_context.context:
                self.execution_context.context["execution_time"] = execution_time

            # Log slow queries
            if execution_time > self.timeout_seconds:
                logger.warning(
                    f"Slow GraphQL query detected: {execution_time:.2f}s",
                    extra={
                        "execution_time": execution_time,
                        "timeout_limit": self.timeout_seconds,
                        "query_hash": hash(str(self.execution_context.query))
                        if self.execution_context.query
                        else None,
                    },
                )


class QueryCacheExtension(Extension):
    """Extension for caching GraphQL query results."""

    def __init__(self, cache_ttl: int = 300, enabled: bool = True):
        self.cache_ttl = cache_ttl
        self.enabled = enabled
        self._cache = {}  # In production, use Redis
        self._cache_timestamps = {}

    def on_request(self) -> None:
        """Called when request starts."""
        if not self.enabled:
            return

        # Generate cache key from query and variables
        cache_key = self._generate_cache_key()

        if cache_key and self._is_cache_valid(cache_key):
            # Return cached result
            cached_result = self._cache.get(cache_key)
            if cached_result:
                logger.debug(f"Cache hit for query: {cache_key[:50]}...")
                # Note: In a real implementation, you'd need to handle
                # returning the cached result properly

    def on_end(self) -> None:
        """Called when request ends."""
        if not self.enabled:
            return

        # Cache successful results
        if (
            hasattr(self.execution_context, "result")
            and self.execution_context.result
            and not self.execution_context.result.errors
        ):
            cache_key = self._generate_cache_key()
            if cache_key:
                self._cache[cache_key] = self.execution_context.result
                self._cache_timestamps[cache_key] = time.time()
                logger.debug(f"Cached result for query: {cache_key[:50]}...")

    def _generate_cache_key(self) -> str | None:
        """Generate cache key from query and variables."""
        try:
            if not self.execution_context.query:
                return None

            query_str = str(self.execution_context.query)
            variables_str = str(self.execution_context.variables or {})

            # Simple cache key generation
            return f"{hash(query_str)}:{hash(variables_str)}"
        except Exception as e:
            logger.error(f"Error generating cache key: {e}")
            return None

    def _is_cache_valid(self, cache_key: str) -> bool:
        """Check if cached result is still valid."""
        if cache_key not in self._cache:
            return False

        timestamp = self._cache_timestamps.get(cache_key, 0)
        return (time.time() - timestamp) < self.cache_ttl


class QueryRateLimitExtension(Extension):
    """Extension for rate limiting GraphQL queries per user."""

    def __init__(
        self, queries_per_minute: int = 60, burst_limit: int = 10, window_seconds: int = 60
    ):
        self.queries_per_minute = queries_per_minute
        self.burst_limit = burst_limit
        self.window_seconds = window_seconds
        self._request_counts = defaultdict(list)  # In production, use Redis

    def on_request(self) -> None:
        """Called when request starts."""
        # Get user identifier from context
        user_id = self._get_user_id()
        if not user_id:
            return

        current_time = time.time()
        window_start = current_time - self.window_seconds

        # Clean old requests
        self._request_counts[user_id] = [
            req_time for req_time in self._request_counts[user_id] if req_time > window_start
        ]

        # Check rate limits
        request_count = len(self._request_counts[user_id])

        if request_count >= self.queries_per_minute:
            raise GraphQLError(
                f"Rate limit exceeded: {request_count} requests in the last {self.window_seconds}s. "
                f"Maximum allowed: {self.queries_per_minute} requests per minute."
            )

        # Add current request
        self._request_counts[user_id].append(current_time)

    def _get_user_id(self) -> str | None:
        """Get user ID from execution context."""
        try:
            if self.execution_context.context and "request" in self.execution_context.context:
                request = self.execution_context.context["request"]
                # Extract user ID from request headers or auth
                return getattr(request.state, "user_id", None)
            return None
        except Exception:
            return None


class SecurityExtensionManager:
    """Manager for GraphQL security extensions."""

    @classmethod
    def get_production_extensions(cls) -> list[Extension]:
        """Get security extensions for production environment."""
        return [
            QueryComplexityExtension(
                max_complexity=500,  # Stricter in production
                max_depth=8,
                introspection_enabled=False,
            ),
            QueryTimeoutExtension(timeout_seconds=15.0),
            QueryRateLimitExtension(
                queries_per_minute=30,  # Stricter in production
                burst_limit=5,
            ),
            QueryCacheExtension(cache_ttl=300, enabled=True),
        ]

    @classmethod
    def get_development_extensions(cls) -> list[Extension]:
        """Get security extensions for development environment."""
        return [
            QueryComplexityExtension(
                max_complexity=2000,  # More lenient in dev
                max_depth=15,
                introspection_enabled=True,
            ),
            QueryTimeoutExtension(timeout_seconds=60.0),
            QueryRateLimitExtension(
                queries_per_minute=120,  # More lenient in dev
                burst_limit=20,
            ),
            QueryCacheExtension(cache_ttl=60, enabled=True),
        ]
