"""Advanced GraphQL Optimizer
Enterprise-grade GraphQL optimizations including:
- N+1 query resolution with intelligent batching
- Query complexity analysis and rate limiting
- Intelligent caching with Redis integration
- Real-time subscriptions with WebSocket support
- GraphQL Federation for microservices
- Performance monitoring and alerting
- Query persistence and whitelisting
- Advanced security features
"""

from __future__ import annotations

import asyncio
import json
import time
from collections import defaultdict, deque
from collections.abc import AsyncIterable, Callable
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from functools import wraps
from typing import Any

import strawberry
from dataloader import DataLoader
from graphql import GraphQLError
from sqlmodel import Session, select
from strawberry.extensions import Extension
from strawberry.types import Info

from core.caching.cache_patterns import get_cache_manager
from core.logging import get_logger
from data_access.db import get_session
from messaging.rabbitmq_manager import RabbitMQManager
from streaming.kafka_manager import KafkaManager

logger = get_logger(__name__)


@dataclass
class QueryMetrics:
    """GraphQL query performance metrics"""

    query_hash: str
    query_name: str | None
    execution_time: float
    database_queries: int
    cache_hits: int
    cache_misses: int
    resolver_count: int
    complexity_score: int
    timestamp: datetime = field(default_factory=datetime.utcnow)
    user_id: str | None = None
    variables: dict[str, Any] = field(default_factory=dict)
    errors: list[str] = field(default_factory=list)


class DataLoaderRegistry:
    """Registry for managing DataLoaders to solve N+1 problems"""

    def __init__(self):
        self.loaders: dict[str, DataLoader] = {}
        self.session_loaders: dict[str, dict[str, DataLoader]] = {}

    def get_or_create_loader(
        self, session_id: str, loader_name: str, batch_load_fn: Callable
    ) -> DataLoader:
        """Get or create a DataLoader for a specific session"""
        if session_id not in self.session_loaders:
            self.session_loaders[session_id] = {}

        if loader_name not in self.session_loaders[session_id]:
            self.session_loaders[session_id][loader_name] = DataLoader(
                batch_load_fn=batch_load_fn, max_batch_size=100, cache=True
            )

        return self.session_loaders[session_id][loader_name]

    def clear_session_loaders(self, session_id: str):
        """Clear all loaders for a session"""
        if session_id in self.session_loaders:
            del self.session_loaders[session_id]


class GraphQLBatchLoader:
    """Batch loader implementation for common database operations"""

    def __init__(self, session: Session):
        self.session = session

    async def batch_load_customers(self, customer_keys: list[int]) -> list[Any | None]:
        """Batch load customers by their keys"""
        from data_access.models.star_schema import DimCustomer

        query = select(DimCustomer).where(DimCustomer.customer_key.in_(customer_keys))
        customers = self.session.exec(query).all()

        # Create lookup dict
        customer_dict = {customer.customer_key: customer for customer in customers}

        # Return in the same order as requested keys
        return [customer_dict.get(key) for key in customer_keys]

    async def batch_load_products(self, product_keys: list[int]) -> list[Any | None]:
        """Batch load products by their keys"""
        from data_access.models.star_schema import DimProduct

        query = select(DimProduct).where(DimProduct.product_key.in_(product_keys))
        products = self.session.exec(query).all()

        # Create lookup dict
        product_dict = {product.product_key: product for product in products}

        return [product_dict.get(key) for key in product_keys]

    async def batch_load_countries(self, country_keys: list[int]) -> list[Any | None]:
        """Batch load countries by their keys"""
        from data_access.models.star_schema import DimCountry

        query = select(DimCountry).where(DimCountry.country_key.in_(country_keys))
        countries = self.session.exec(query).all()

        country_dict = {country.country_key: country for country in countries}

        return [country_dict.get(key) for key in country_keys]

    async def batch_load_sales_by_customer(self, customer_keys: list[int]) -> list[list[Any]]:
        """Batch load sales by customer keys"""
        from data_access.models.star_schema import FactSale

        query = select(FactSale).where(FactSale.customer_key.in_(customer_keys))
        sales = self.session.exec(query).all()

        # Group sales by customer_key
        sales_by_customer = defaultdict(list)
        for sale in sales:
            sales_by_customer[sale.customer_key].append(sale)

        return [sales_by_customer.get(key, []) for key in customer_keys]


class QueryComplexityAnalyzer:
    """Analyze and limit GraphQL query complexity"""

    def __init__(self, max_complexity: int = 1000, max_depth: int = 10):
        self.max_complexity = max_complexity
        self.max_depth = max_depth

    def analyze_query(self, query: str, variables: dict[str, Any] = None) -> dict[str, Any]:
        """Analyze query complexity and depth"""
        # Simplified complexity analysis - in production, use graphql-query-complexity
        complexity_score = self._calculate_complexity(query)
        depth_score = self._calculate_depth(query)

        return {
            "complexity": complexity_score,
            "depth": depth_score,
            "is_valid": complexity_score <= self.max_complexity and depth_score <= self.max_depth,
            "estimated_cost": self._estimate_cost(complexity_score, depth_score),
        }

    def _calculate_complexity(self, query: str) -> int:
        """Calculate query complexity based on field count and nesting"""
        # Simple heuristic: count braces and field selections
        field_count = query.count("{")
        list_fields = query.count("[") * 10  # Lists are more expensive
        return field_count + list_fields

    def _calculate_depth(self, query: str) -> int:
        """Calculate query depth"""
        # Simple heuristic: count maximum nesting level
        max_depth = 0
        current_depth = 0

        for char in query:
            if char == "{":
                current_depth += 1
                max_depth = max(max_depth, current_depth)
            elif char == "}":
                current_depth -= 1

        return max_depth

    def _estimate_cost(self, complexity: int, depth: int) -> float:
        """Estimate query execution cost"""
        base_cost = complexity * 0.1
        depth_penalty = depth**1.5
        return base_cost + depth_penalty


class GraphQLCacheExtension(Extension):
    """GraphQL extension for intelligent caching"""

    def __init__(self):
        super().__init__()
        self.cache_manager = get_cache_manager()
        self.cache_ttl = 300  # 5 minutes default TTL

    async def on_request_start(self):
        """Called when GraphQL request starts"""
        self.request_start_time = time.time()

    async def on_request_end(self):
        """Called when GraphQL request ends"""
        execution_time = time.time() - self.request_start_time

        # Log slow queries
        if execution_time > 1.0:  # Log queries taking more than 1 second
            logger.warning(
                f"Slow GraphQL query detected: {execution_time:.3f}s",
                extra={"execution_time": execution_time},
            )

    def should_cache_result(self, info: Info) -> bool:
        """Determine if query result should be cached"""
        # Cache only queries (not mutations)
        if info.operation.operation == "query":
            # Don't cache real-time data
            query_name = getattr(info.field_name, "lower", lambda: "")() or ""
            if "realtime" not in query_name and "live" not in query_name:
                return True
        return False

    async def get_cached_result(self, cache_key: str) -> Any | None:
        """Get cached query result"""
        try:
            cached_data = await self.cache_manager.get(f"graphql:{cache_key}")
            if cached_data:
                return json.loads(cached_data)
        except Exception as e:
            logger.warning(f"Cache get error: {e}")
        return None

    async def cache_result(self, cache_key: str, result: Any, ttl: int = None):
        """Cache query result"""
        try:
            ttl = ttl or self.cache_ttl
            await self.cache_manager.set(
                f"graphql:{cache_key}", json.dumps(result, default=str), ttl=ttl
            )
        except Exception as e:
            logger.warning(f"Cache set error: {e}")

    def generate_cache_key(self, info: Info, variables: dict[str, Any]) -> str:
        """Generate cache key for query"""
        import hashlib

        # Create deterministic cache key from query and variables
        query_str = str(info.query)
        variables_str = json.dumps(variables or {}, sort_keys=True)

        key_data = f"{query_str}:{variables_str}"
        return hashlib.md5(key_data.encode()).hexdigest()


class GraphQLMetricsExtension(Extension):
    """GraphQL extension for comprehensive metrics collection"""

    def __init__(self):
        super().__init__()
        self.metrics_queue = deque(maxlen=10000)
        self.complexity_analyzer = QueryComplexityAnalyzer()
        self.kafka_manager = KafkaManager()

    async def on_request_start(self):
        """Initialize request metrics"""
        self.request_start_time = time.time()
        self.database_query_count = 0
        self.cache_hits = 0
        self.cache_misses = 0
        self.resolver_count = 0

    async def on_request_end(self):
        """Record request metrics"""
        execution_time = time.time() - self.request_start_time

        metrics = QueryMetrics(
            query_hash=self._generate_query_hash(),
            query_name=getattr(self, "query_name", None),
            execution_time=execution_time,
            database_queries=self.database_query_count,
            cache_hits=self.cache_hits,
            cache_misses=self.cache_misses,
            resolver_count=self.resolver_count,
            complexity_score=getattr(self, "complexity_score", 0),
        )

        # Store metrics
        self.metrics_queue.append(metrics)

        # Publish metrics to Kafka for monitoring
        await self._publish_metrics(metrics)

    def _generate_query_hash(self) -> str:
        """Generate hash for the current query"""
        import hashlib

        query_str = getattr(self, "query_string", "unknown")
        return hashlib.md5(query_str.encode()).hexdigest()[:16]

    async def _publish_metrics(self, metrics: QueryMetrics):
        """Publish metrics to Kafka"""
        try:
            metrics_data = {
                "query_hash": metrics.query_hash,
                "query_name": metrics.query_name,
                "execution_time_ms": metrics.execution_time * 1000,
                "database_queries": metrics.database_queries,
                "cache_hits": metrics.cache_hits,
                "cache_misses": metrics.cache_misses,
                "complexity_score": metrics.complexity_score,
                "timestamp": metrics.timestamp.isoformat(),
            }

            self.kafka_manager.produce_graphql_metrics(**metrics_data)

        except Exception as e:
            logger.error(f"Failed to publish GraphQL metrics: {e}")

    def get_metrics_summary(self) -> dict[str, Any]:
        """Get aggregated metrics summary"""
        if not self.metrics_queue:
            return {}

        total_requests = len(self.metrics_queue)
        avg_execution_time = sum(m.execution_time for m in self.metrics_queue) / total_requests
        total_db_queries = sum(m.database_queries for m in self.metrics_queue)
        total_cache_hits = sum(m.cache_hits for m in self.metrics_queue)
        total_cache_misses = sum(m.cache_misses for m in self.metrics_queue)

        cache_hit_rate = (
            total_cache_hits / (total_cache_hits + total_cache_misses) * 100
            if (total_cache_hits + total_cache_misses) > 0
            else 0
        )

        return {
            "total_requests": total_requests,
            "avg_execution_time_ms": avg_execution_time * 1000,
            "total_database_queries": total_db_queries,
            "cache_hit_rate_percent": cache_hit_rate,
            "queries_per_request": total_db_queries / total_requests if total_requests > 0 else 0,
        }


class GraphQLSubscriptionManager:
    """Manager for GraphQL subscriptions and real-time updates"""

    def __init__(self):
        self.active_subscriptions: dict[str, dict[str, Any]] = {}
        self.subscription_topics: dict[str, set[str]] = defaultdict(set)
        self.rabbitmq_manager = RabbitMQManager()
        self.kafka_manager = KafkaManager()

        # Start background task for subscription processing
        asyncio.create_task(self._process_subscription_events())

    async def subscribe(
        self,
        subscription_id: str,
        topic: str,
        query: str,
        variables: dict[str, Any],
        websocket_send: Callable,
    ) -> bool:
        """Register a new subscription"""
        try:
            self.active_subscriptions[subscription_id] = {
                "topic": topic,
                "query": query,
                "variables": variables,
                "websocket_send": websocket_send,
                "created_at": datetime.utcnow(),
                "last_update": datetime.utcnow(),
            }

            self.subscription_topics[topic].add(subscription_id)

            logger.info(f"GraphQL subscription registered: {subscription_id} for topic: {topic}")
            return True

        except Exception as e:
            logger.error(f"Failed to register subscription {subscription_id}: {e}")
            return False

    async def unsubscribe(self, subscription_id: str) -> bool:
        """Unregister a subscription"""
        try:
            if subscription_id in self.active_subscriptions:
                subscription = self.active_subscriptions[subscription_id]
                topic = subscription["topic"]

                # Remove from topic mapping
                self.subscription_topics[topic].discard(subscription_id)

                # Remove subscription
                del self.active_subscriptions[subscription_id]

                logger.info(f"GraphQL subscription removed: {subscription_id}")
                return True

            return False

        except Exception as e:
            logger.error(f"Failed to unregister subscription {subscription_id}: {e}")
            return False

    async def publish_update(self, topic: str, data: Any):
        """Publish update to all subscribers of a topic"""
        if topic not in self.subscription_topics:
            return

        subscription_ids = self.subscription_topics[topic].copy()

        for subscription_id in subscription_ids:
            if subscription_id in self.active_subscriptions:
                subscription = self.active_subscriptions[subscription_id]

                try:
                    # Send update via WebSocket
                    await subscription["websocket_send"](
                        {"type": "data", "id": subscription_id, "payload": {"data": data}}
                    )

                    subscription["last_update"] = datetime.utcnow()

                except Exception as e:
                    logger.error(f"Failed to send update to subscription {subscription_id}: {e}")
                    # Remove failed subscription
                    await self.unsubscribe(subscription_id)

    async def _process_subscription_events(self):
        """Background task to process subscription events from message queues"""

        def handle_event(event_data):
            """Handle incoming subscription event"""
            try:
                topic = event_data.get("topic")
                data = event_data.get("data")

                if topic and data:
                    asyncio.create_task(self.publish_update(topic, data))

            except Exception as e:
                logger.error(f"Error processing subscription event: {e}")

        # Subscribe to subscription events from RabbitMQ
        self.rabbitmq_manager.subscribe_to_events(
            event_types=["subscription_update"], callback=handle_event
        )

    def get_subscription_stats(self) -> dict[str, Any]:
        """Get subscription statistics"""
        active_count = len(self.active_subscriptions)
        topics_count = len(self.subscription_topics)

        topic_stats = {topic: len(subs) for topic, subs in self.subscription_topics.items()}

        return {
            "active_subscriptions": active_count,
            "active_topics": topics_count,
            "subscriptions_per_topic": topic_stats,
        }


class GraphQLFederationSupport:
    """Support for GraphQL Federation across microservices"""

    def __init__(self):
        self.service_schemas: dict[str, Any] = {}
        self.entity_resolvers: dict[str, Callable] = {}
        self.federation_directives = {"key", "external", "requires", "provides", "extends"}

    def register_service_schema(self, service_name: str, schema_sdl: str, gateway_url: str):
        """Register a service schema for federation"""
        self.service_schemas[service_name] = {
            "schema_sdl": schema_sdl,
            "gateway_url": gateway_url,
            "registered_at": datetime.utcnow(),
        }

        logger.info(f"Registered federated schema for service: {service_name}")

    def register_entity_resolver(self, type_name: str, resolver: Callable):
        """Register entity resolver for federation"""
        self.entity_resolvers[type_name] = resolver
        logger.info(f"Registered entity resolver for type: {type_name}")

    async def resolve_entity(self, typename: str, representation: dict[str, Any]) -> Any:
        """Resolve federated entity"""
        if typename in self.entity_resolvers:
            resolver = self.entity_resolvers[typename]
            return await resolver(representation)

        logger.warning(f"No entity resolver found for type: {typename}")
        return None

    def build_federated_schema(self) -> str:
        """Build complete federated schema SDL"""
        # Combine all service schemas with federation directives
        combined_sdl = "# Federated GraphQL Schema\n\n"

        for service_name, schema_info in self.service_schemas.items():
            combined_sdl += f"# Schema from {service_name}\n"
            combined_sdl += schema_info["schema_sdl"]
            combined_sdl += "\n\n"

        return combined_sdl

    def get_federation_stats(self) -> dict[str, Any]:
        """Get federation statistics"""
        return {
            "registered_services": len(self.service_schemas),
            "entity_resolvers": len(self.entity_resolvers),
            "services": list(self.service_schemas.keys()),
        }


class OptimizedGraphQLContext:
    """Enhanced GraphQL context with optimization features"""

    def __init__(self, request: Any, session: Session):
        self.request = request
        self.session = session
        self.session_id = getattr(request, "session_id", str(time.time()))

        # Initialize optimization components
        self.dataloader_registry = DataLoaderRegistry()
        self.batch_loader = GraphQLBatchLoader(session)
        self.cache_extension = GraphQLCacheExtension()

        # Request-specific data
        self.user_id = getattr(request, "user_id", None)
        self.request_start_time = time.time()
        self.complexity_analyzer = QueryComplexityAnalyzer()

    def get_customer_loader(self) -> DataLoader:
        """Get customer DataLoader for N+1 prevention"""
        return self.dataloader_registry.get_or_create_loader(
            self.session_id, "customers", self.batch_loader.batch_load_customers
        )

    def get_product_loader(self) -> DataLoader:
        """Get product DataLoader for N+1 prevention"""
        return self.dataloader_registry.get_or_create_loader(
            self.session_id, "products", self.batch_loader.batch_load_products
        )

    def get_country_loader(self) -> DataLoader:
        """Get country DataLoader for N+1 prevention"""
        return self.dataloader_registry.get_or_create_loader(
            self.session_id, "countries", self.batch_loader.batch_load_countries
        )

    def get_sales_by_customer_loader(self) -> DataLoader:
        """Get sales by customer DataLoader"""
        return self.dataloader_registry.get_or_create_loader(
            self.session_id, "sales_by_customer", self.batch_loader.batch_load_sales_by_customer
        )

    async def cleanup(self):
        """Clean up context resources"""
        self.dataloader_registry.clear_session_loaders(self.session_id)
        self.session.close()


# Decorators for GraphQL optimizations
def cached_resolver(ttl: int = 300, key_func: Callable = None):
    """Decorator for caching GraphQL resolver results"""

    def decorator(resolver_func: Callable) -> Callable:
        @wraps(resolver_func)
        async def wrapper(self, info: Info, *args, **kwargs):
            cache_manager = get_cache_manager()

            # Generate cache key
            if key_func:
                cache_key = key_func(info, args, kwargs)
            else:
                cache_key = f"resolver:{resolver_func.__name__}:{hash(str(args) + str(kwargs))}"

            # Try to get from cache
            cached_result = await cache_manager.get(cache_key)
            if cached_result is not None:
                return json.loads(cached_result)

            # Execute resolver
            result = await resolver_func(self, info, *args, **kwargs)

            # Cache result
            await cache_manager.set(cache_key, json.dumps(result, default=str), ttl=ttl)

            return result

        return wrapper

    return decorator


def dataloader_resolver(loader_name: str):
    """Decorator for using DataLoader in resolver"""

    def decorator(resolver_func: Callable) -> Callable:
        @wraps(resolver_func)
        async def wrapper(self, info: Info, *args, **kwargs):
            context = info.context

            if hasattr(context, "get_dataloader"):
                loader = getattr(context, f"get_{loader_name}_loader")()
                return await resolver_func(self, info, loader, *args, **kwargs)
            else:
                return await resolver_func(self, info, None, *args, **kwargs)

        return wrapper

    return decorator


def complexity_limit(max_complexity: int = 1000):
    """Decorator for limiting query complexity"""

    def decorator(resolver_func: Callable) -> Callable:
        @wraps(resolver_func)
        async def wrapper(self, info: Info, *args, **kwargs):
            analyzer = QueryComplexityAnalyzer(max_complexity=max_complexity)
            query_str = str(info.query)

            analysis = analyzer.analyze_query(query_str, info.variable_values)

            if not analysis["is_valid"]:
                raise GraphQLError(
                    f"Query complexity {analysis['complexity']} exceeds limit {max_complexity}"
                )

            return await resolver_func(self, info, *args, **kwargs)

        return wrapper

    return decorator


class GraphQLOptimizer:
    """Main GraphQL optimization coordinator"""

    def __init__(self):
        self.metrics_extension = GraphQLMetricsExtension()
        self.cache_extension = GraphQLCacheExtension()
        self.subscription_manager = GraphQLSubscriptionManager()
        self.federation_support = GraphQLFederationSupport()
        self.thread_pool = ThreadPoolExecutor(max_workers=10)

    def create_optimized_context(self, request: Any) -> OptimizedGraphQLContext:
        """Create optimized GraphQL context"""
        session = next(get_session())
        return OptimizedGraphQLContext(request, session)

    def get_schema_extensions(self) -> list[Extension]:
        """Get all GraphQL extensions for optimization"""
        return [self.metrics_extension, self.cache_extension]

    async def handle_subscription(
        self, websocket, subscription_id: str, query: str, variables: dict[str, Any]
    ):
        """Handle GraphQL subscription"""
        # Extract subscription topic from query
        topic = self._extract_subscription_topic(query)

        async def websocket_send(message):
            await websocket.send_json(message)

        # Register subscription
        success = await self.subscription_manager.subscribe(
            subscription_id, topic, query, variables, websocket_send
        )

        return success

    def _extract_subscription_topic(self, query: str) -> str:
        """Extract topic from subscription query"""
        # Simple extraction - in production, parse the AST
        if "salesUpdates" in query:
            return "sales_updates"
        elif "taskStatus" in query:
            return "task_updates"
        else:
            return "general_updates"

    async def get_comprehensive_metrics(self) -> dict[str, Any]:
        """Get comprehensive GraphQL metrics"""
        return {
            "query_metrics": self.metrics_extension.get_metrics_summary(),
            "subscription_stats": self.subscription_manager.get_subscription_stats(),
            "federation_stats": self.federation_support.get_federation_stats(),
            "cache_stats": await self.cache_extension.cache_manager.get_stats(),
            "timestamp": datetime.utcnow().isoformat(),
        }

    async def cleanup(self):
        """Clean up optimizer resources"""
        self.thread_pool.shutdown(wait=True)
        logger.info("GraphQL optimizer cleaned up")


# Global optimizer instance
_graphql_optimizer: GraphQLOptimizer | None = None


def get_graphql_optimizer() -> GraphQLOptimizer:
    """Get or create global GraphQL optimizer instance"""
    global _graphql_optimizer
    if _graphql_optimizer is None:
        _graphql_optimizer = GraphQLOptimizer()
    return _graphql_optimizer


# Example optimized resolvers using the new patterns
@strawberry.type
class OptimizedSale:
    """Optimized Sale type with DataLoader support"""

    sale_id: str
    quantity: int
    unit_price: float
    total_amount: float

    @strawberry.field
    @dataloader_resolver("customer")
    async def customer(self, info: Info, loader: DataLoader | None) -> Any | None:
        """Get customer using DataLoader to prevent N+1"""
        if loader and hasattr(self, "customer_key"):
            return await loader.load(self.customer_key)
        return None

    @strawberry.field
    @dataloader_resolver("product")
    async def product(self, info: Info, loader: DataLoader | None) -> Any | None:
        """Get product using DataLoader to prevent N+1"""
        if loader and hasattr(self, "product_key"):
            return await loader.load(self.product_key)
        return None

    @strawberry.field
    @cached_resolver(ttl=600)  # Cache for 10 minutes
    async def calculated_metrics(self, info: Info) -> dict[str, float]:
        """Get calculated metrics with caching"""
        return {
            "profit_margin": (self.total_amount - (self.unit_price * 0.7))
            / self.total_amount
            * 100,
            "revenue_impact": self.total_amount * 1.2,
        }


# Subscription types for real-time updates
@strawberry.type
class Subscription:
    """GraphQL Subscription root"""

    @strawberry.subscription
    async def sales_updates(self, info: Info, filters: Any | None = None) -> AsyncIterable[Any]:
        """Subscribe to real-time sales updates"""
        # This would be implemented with proper async iteration
        # For now, it's a placeholder
        yield {"message": "Sales update subscription"}

    @strawberry.subscription
    async def task_status_updates(self, info: Info, task_id: str) -> AsyncIterable[Any]:
        """Subscribe to task status updates"""
        yield {"task_id": task_id, "status": "running"}


class QueryPersistenceManager:
    """Manager for persisted queries and query whitelisting"""

    def __init__(self):
        self.persisted_queries: dict[str, str] = {}
        self.query_whitelist: set[str] = set()
        self.cache_manager = get_cache_manager()
        self.enabled = True

    async def store_persisted_query(self, query_hash: str, query: str) -> bool:
        """Store a persisted query"""
        try:
            self.persisted_queries[query_hash] = query
            await self.cache_manager.set(
                f"persisted_query:{query_hash}",
                query,
                ttl=86400,  # 24 hours
            )
            logger.info(f"Stored persisted query: {query_hash}")
            return True
        except Exception as e:
            logger.error(f"Failed to store persisted query {query_hash}: {e}")
            return False

    async def get_persisted_query(self, query_hash: str) -> str | None:
        """Retrieve a persisted query"""
        try:
            # First check memory cache
            if query_hash in self.persisted_queries:
                return self.persisted_queries[query_hash]

            # Then check Redis cache
            cached_query = await self.cache_manager.get(f"persisted_query:{query_hash}")
            if cached_query:
                self.persisted_queries[query_hash] = cached_query
                return cached_query

            return None

        except Exception as e:
            logger.error(f"Failed to get persisted query {query_hash}: {e}")
            return None

    def add_to_whitelist(self, query_hash: str):
        """Add query to whitelist"""
        self.query_whitelist.add(query_hash)
        logger.info(f"Added query to whitelist: {query_hash}")

    def is_query_allowed(self, query_hash: str) -> bool:
        """Check if query is allowed based on whitelist"""
        if not self.enabled:
            return True

        return len(self.query_whitelist) == 0 or query_hash in self.query_whitelist

    async def load_whitelist_from_config(self, config_path: str):
        """Load query whitelist from configuration file"""
        try:
            import json

            with open(config_path) as f:
                config = json.load(f)
                self.query_whitelist.update(config.get("allowed_queries", []))
            logger.info(f"Loaded {len(self.query_whitelist)} queries into whitelist")
        except Exception as e:
            logger.error(f"Failed to load whitelist from {config_path}: {e}")


class GraphQLSecurityExtension(Extension):
    """GraphQL extension for advanced security features"""

    def __init__(self):
        super().__init__()
        self.rate_limiter = {}  # Simple in-memory rate limiter
        self.blocked_queries = set()
        self.query_persistence = QueryPersistenceManager()
        self.introspection_enabled = False  # Disable in production

    async def on_request_start(self):
        """Security checks before request processing"""
        self.request_start_time = time.time()

    async def on_validate(self, schema, document, **kwargs):
        """Validate query security"""
        # Check for introspection queries in production
        if not self.introspection_enabled:
            if self._contains_introspection(document):
                raise GraphQLError("Introspection queries are not allowed")

        # Check query complexity and depth
        query_str = str(document)
        query_hash = self._generate_query_hash(query_str)

        # Check if query is blocked
        if query_hash in self.blocked_queries:
            raise GraphQLError("This query has been blocked")

        # Check whitelist
        if not self.query_persistence.is_query_allowed(query_hash):
            raise GraphQLError("Query not in whitelist")

        # Rate limiting check
        if not self._check_rate_limit("global", 100, 60):  # 100 requests per minute
            raise GraphQLError("Rate limit exceeded")

    def _contains_introspection(self, document) -> bool:
        """Check if query contains introspection"""
        query_str = str(document).lower()
        introspection_keywords = ["__schema", "__type", "__typename", "__field", "__directive"]
        return any(keyword in query_str for keyword in introspection_keywords)

    def _generate_query_hash(self, query: str) -> str:
        """Generate hash for query"""
        import hashlib

        return hashlib.sha256(query.encode()).hexdigest()[:32]

    def _check_rate_limit(self, key: str, limit: int, window: int) -> bool:
        """Simple rate limiting check"""
        now = time.time()

        if key not in self.rate_limiter:
            self.rate_limiter[key] = []

        # Remove old entries
        self.rate_limiter[key] = [
            timestamp for timestamp in self.rate_limiter[key] if now - timestamp < window
        ]

        # Check if under limit
        if len(self.rate_limiter[key]) >= limit:
            return False

        # Add current request
        self.rate_limiter[key].append(now)
        return True

    def block_query(self, query_hash: str, reason: str = "Security violation"):
        """Block a specific query"""
        self.blocked_queries.add(query_hash)
        logger.warning(f"Blocked query {query_hash}: {reason}")


class GraphQLPerformanceProfiler:
    """Advanced performance profiling for GraphQL operations"""

    def __init__(self):
        self.profiles: dict[str, dict[str, Any]] = {}
        self.slow_query_threshold = 2.0  # seconds
        self.profiling_enabled = True

    async def start_profiling(self, operation_name: str) -> str:
        """Start profiling a GraphQL operation"""
        profile_id = f"{operation_name}_{int(time.time() * 1000)}"

        self.profiles[profile_id] = {
            "operation_name": operation_name,
            "start_time": time.time(),
            "resolvers": {},
            "database_queries": [],
            "cache_operations": [],
            "external_calls": [],
        }

        return profile_id

    async def end_profiling(self, profile_id: str) -> dict[str, Any]:
        """End profiling and return results"""
        if profile_id not in self.profiles:
            return {}

        profile = self.profiles[profile_id]
        total_time = time.time() - profile["start_time"]

        profile["total_time"] = total_time
        profile["is_slow_query"] = total_time > self.slow_query_threshold

        # Calculate resolver breakdown
        resolver_times = profile["resolvers"]
        if resolver_times:
            profile["slowest_resolver"] = max(
                resolver_times.items(), key=lambda x: x[1]["duration"]
            )

        # Log slow queries
        if profile["is_slow_query"]:
            logger.warning(
                f"Slow GraphQL operation: {profile['operation_name']} - {total_time:.3f}s",
                extra={
                    "operation_name": profile["operation_name"],
                    "total_time": total_time,
                    "profile_data": profile,
                },
            )

        # Clean up
        del self.profiles[profile_id]

        return profile

    def track_resolver(self, profile_id: str, resolver_name: str, duration: float, **metadata):
        """Track resolver execution"""
        if profile_id in self.profiles:
            self.profiles[profile_id]["resolvers"][resolver_name] = {
                "duration": duration,
                "metadata": metadata,
                "timestamp": time.time(),
            }

    def track_database_query(self, profile_id: str, query: str, duration: float, result_count: int):
        """Track database query execution"""
        if profile_id in self.profiles:
            self.profiles[profile_id]["database_queries"].append(
                {
                    "query": query[:200] + "..." if len(query) > 200 else query,
                    "duration": duration,
                    "result_count": result_count,
                    "timestamp": time.time(),
                }
            )

    def track_cache_operation(
        self, profile_id: str, operation: str, key: str, hit: bool, duration: float
    ):
        """Track cache operation"""
        if profile_id in self.profiles:
            self.profiles[profile_id]["cache_operations"].append(
                {
                    "operation": operation,
                    "key": key,
                    "hit": hit,
                    "duration": duration,
                    "timestamp": time.time(),
                }
            )

    def get_performance_summary(self) -> dict[str, Any]:
        """Get performance summary across all operations"""
        active_profiles = len(self.profiles)

        # Calculate averages from completed operations (would need to store history)
        return {
            "active_profiles": active_profiles,
            "profiling_enabled": self.profiling_enabled,
            "slow_query_threshold": self.slow_query_threshold,
        }


class EnhancedGraphQLOptimizer(GraphQLOptimizer):
    """Enhanced GraphQL optimizer with advanced enterprise features"""

    def __init__(self):
        super().__init__()
        self.security_extension = GraphQLSecurityExtension()
        self.profiler = GraphQLPerformanceProfiler()
        self.query_persistence = QueryPersistenceManager()
        self.connection_pool_size = 50
        self.max_query_timeout = 30  # seconds

    def get_production_extensions(self) -> list[Extension]:
        """Get all extensions for production deployment"""
        return [self.security_extension, self.metrics_extension, self.cache_extension]

    async def execute_with_profiling(
        self, query: str, variables: dict[str, Any], operation_name: str = None
    ) -> dict[str, Any]:
        """Execute GraphQL query with comprehensive profiling"""
        profile_id = await self.profiler.start_profiling(operation_name or "anonymous_operation")

        try:
            # Execute query (this would integrate with your GraphQL execution engine)
            start_time = time.time()

            # Placeholder for actual GraphQL execution
            result = {"data": {}, "errors": []}

            execution_time = time.time() - start_time

            # End profiling
            profile = await self.profiler.end_profiling(profile_id)

            # Add profiling data to result
            if profile:
                result["extensions"] = {"profile": profile, "execution_time": execution_time}

            return result

        except Exception as e:
            await self.profiler.end_profiling(profile_id)
            raise e

    async def handle_persisted_query(
        self, query_hash: str, variables: dict[str, Any]
    ) -> dict[str, Any]:
        """Handle persisted query execution"""
        query = await self.query_persistence.get_persisted_query(query_hash)

        if not query:
            raise GraphQLError(f"Persisted query not found: {query_hash}")

        return await self.execute_with_profiling(
            query, variables, f"persisted_query_{query_hash[:8]}"
        )

    async def store_query_for_persistence(self, query: str) -> str:
        """Store query for persistence and return hash"""
        import hashlib

        query_hash = hashlib.sha256(query.encode()).hexdigest()[:32]

        success = await self.query_persistence.store_persisted_query(query_hash, query)

        if success:
            # Optionally add to whitelist
            self.query_persistence.add_to_whitelist(query_hash)
            return query_hash
        else:
            raise Exception("Failed to store persisted query")

    async def get_advanced_metrics(self) -> dict[str, Any]:
        """Get comprehensive advanced metrics"""
        base_metrics = await self.get_comprehensive_metrics()

        return {
            **base_metrics,
            "security_stats": {
                "blocked_queries": len(self.security_extension.blocked_queries),
                "whitelist_size": len(self.query_persistence.query_whitelist),
                "persisted_queries": len(self.query_persistence.persisted_queries),
                "rate_limit_entries": len(self.security_extension.rate_limiter),
            },
            "performance_stats": self.profiler.get_performance_summary(),
            "connection_pool": {
                "size": self.connection_pool_size,
                "max_timeout": self.max_query_timeout,
            },
        }

    async def health_check(self) -> dict[str, Any]:
        """Comprehensive health check for GraphQL optimizer"""
        try:
            # Test cache connectivity
            cache_healthy = await self._test_cache_connectivity()

            # Test database connectivity
            db_healthy = await self._test_database_connectivity()

            # Test message queue connectivity
            mq_healthy = await self._test_message_queue_connectivity()

            # Calculate overall health
            overall_healthy = all([cache_healthy, db_healthy, mq_healthy])

            return {
                "status": "healthy" if overall_healthy else "degraded",
                "components": {
                    "cache": "healthy" if cache_healthy else "unhealthy",
                    "database": "healthy" if db_healthy else "unhealthy",
                    "message_queue": "healthy" if mq_healthy else "unhealthy",
                },
                "metrics": await self.get_advanced_metrics(),
                "timestamp": datetime.utcnow().isoformat(),
            }

        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return {
                "status": "unhealthy",
                "error": str(e),
                "timestamp": datetime.utcnow().isoformat(),
            }

    async def _test_cache_connectivity(self) -> bool:
        """Test cache connectivity"""
        try:
            await self.cache_extension.cache_manager.set("health_check", "ok", ttl=10)
            result = await self.cache_extension.cache_manager.get("health_check")
            return result == "ok"
        except Exception:
            return False

    async def _test_database_connectivity(self) -> bool:
        """Test database connectivity"""
        try:
            session = next(get_session())
            session.exec("SELECT 1")
            session.close()
            return True
        except Exception:
            return False

    async def _test_message_queue_connectivity(self) -> bool:
        """Test message queue connectivity"""
        try:
            # Test both RabbitMQ and Kafka if available
            rabbitmq_healthy = await self.subscription_manager.rabbitmq_manager.health_check()
            kafka_healthy = await self.subscription_manager.kafka_manager.health_check()
            return rabbitmq_healthy or kafka_healthy
        except Exception:
            return False


# Export enhanced optimizer
def get_enhanced_graphql_optimizer() -> EnhancedGraphQLOptimizer:
    """Get enhanced GraphQL optimizer instance"""
    return EnhancedGraphQLOptimizer()
