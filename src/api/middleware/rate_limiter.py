"""
Advanced rate limiting middleware with RabbitMQ backend
Enterprise-grade rate limiting with distributed state management using RabbitMQ
"""
import json
import time
from typing import Any
from collections import defaultdict
from datetime import datetime, timedelta

from fastapi import HTTPException, Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

from messaging.rabbitmq_manager import RabbitMQManager, QueueType, StateMessage
from streaming.kafka_manager import KafkaManager, StreamingTopic
from core.logging import get_logger

logger = get_logger(__name__)


class RateLimitExceeded(HTTPException):
    def __init__(self, detail: str = "Rate limit exceeded", retry_after: int | None = None):
        super().__init__(status_code=429, detail=detail)
        self.retry_after = retry_after


class RateLimitTier:
    """Rate limiting tier configuration."""
    def __init__(self, name: str, limit: int, window: int, burst_limit: int = None):
        self.name = name
        self.limit = limit
        self.window = window
        self.burst_limit = burst_limit or limit


class AdaptiveRateLimiter:
    """Adaptive rate limiter that adjusts limits based on system load and user behavior."""
    
    def __init__(self):
        self.base_limits = {}
        self.user_behavior_scores = defaultdict(float)  # Higher score = better behavior
        self.system_load_factor = 1.0  # Multiplier for system load
        
    def calculate_adaptive_limit(self, base_limit: int, user_id: str) -> int:
        """Calculate adaptive limit based on user behavior and system load."""
        behavior_score = self.user_behavior_scores.get(user_id, 1.0)
        
        # Better behavior gets higher limits (score > 1.0)
        # Worse behavior gets lower limits (score < 1.0)
        behavior_multiplier = min(2.0, max(0.1, behavior_score))
        
        # System load affects limits (high load = lower limits)
        load_multiplier = min(1.0, max(0.1, 2.0 - self.system_load_factor))
        
        adaptive_limit = int(base_limit * behavior_multiplier * load_multiplier)
        return max(1, adaptive_limit)  # Ensure at least 1 request is allowed
    
    def update_user_behavior(self, user_id: str, behavior_score_delta: float):
        """Update user behavior score."""
        current_score = self.user_behavior_scores[user_id]
        # Exponential moving average for behavior scores
        self.user_behavior_scores[user_id] = 0.7 * current_score + 0.3 * (1.0 + behavior_score_delta)
    
    def update_system_load(self, load_factor: float):
        """Update system load factor (0.0 = no load, 2.0 = very high load)."""
        self.system_load_factor = max(0.0, min(2.0, load_factor))


class DistributedSlidingWindowRateLimiter:
    """Advanced sliding window rate limiter with adaptive capabilities using RabbitMQ for distributed state management."""
    
    def __init__(self, rabbitmq_manager: RabbitMQManager, kafka_manager: KafkaManager):
        self.rabbitmq = rabbitmq_manager
        self.kafka = kafka_manager
        
        # Local cache for performance (reduces RabbitMQ calls)
        self.local_state: dict[str, dict[str, Any]] = defaultdict(lambda: {
            'requests': [],
            'last_updated': time.time()
        })
        
        # Configuration
        self.local_cache_ttl = 60  # Cache state locally for 60 seconds
        self.cleanup_interval = 300  # Clean up old entries every 5 minutes
        self.last_cleanup = time.time()
        
        # Advanced features
        self.adaptive_limiter = AdaptiveRateLimiter()
        self.rate_limit_tiers = {
            'basic': RateLimitTier('basic', 100, 60, 150),
            'premium': RateLimitTier('premium', 500, 60, 750),
            'enterprise': RateLimitTier('enterprise', 2000, 60, 3000)
        }
        
        # Metrics tracking
        self.metrics = {
            'total_requests': 0,
            'blocked_requests': 0,
            'adaptive_adjustments': 0
        }

    def _cleanup_local_state(self):
        """Clean up expired entries from local state."""
        current_time = time.time()
        
        if current_time - self.last_cleanup < self.cleanup_interval:
            return
            
        expired_keys = []
        for key, state in self.local_state.items():
            if current_time - state['last_updated'] > self.local_cache_ttl * 2:
                expired_keys.append(key)
        
        for key in expired_keys:
            del self.local_state[key]
            
        self.last_cleanup = current_time

    def _get_local_state(self, key: str, window: int) -> tuple[list[float], bool]:
        """Get local state and determine if it's still valid."""
        current_time = time.time()
        self._cleanup_local_state()
        
        if key not in self.local_state:
            return [], False
            
        state = self.local_state[key]
        
        # Check if local state is still fresh
        if current_time - state['last_updated'] > self.local_cache_ttl:
            return state['requests'], False
            
        # Filter out expired requests
        window_start = current_time - window
        valid_requests = [req_time for req_time in state['requests'] if req_time > window_start]
        state['requests'] = valid_requests
        
        return valid_requests, True

    def _update_local_state(self, key: str, requests: list[float]):
        """Update local state cache."""
        self.local_state[key] = {
            'requests': requests,
            'last_updated': time.time()
        }

    async def _publish_state_update(self, key: str, requests: list[float], window: int):
        """Publish state update to RabbitMQ for distributed synchronization."""
        try:
            state_data = {
                'requests': requests,
                'window': window,
                'updated_at': time.time()
            }
            
            self.rabbitmq.publish_state_update(
                component="rate_limiter",
                key=key,
                value=state_data,
                ttl=window + 60,  # TTL slightly longer than window
                metadata={'component_version': '2.0'}
            )
        except Exception as e:
            # Log error but don't fail the rate limiting
            import logging
            logging.error(f"Failed to publish rate limit state update: {e}")

    async def get_metrics(self) -> dict[str, Any]:
        """Get rate limiter metrics."""
        total = self.metrics['total_requests']
        blocked = self.metrics['blocked_requests']
        
        return {
            'total_requests': total,
            'blocked_requests': blocked,
            'allowed_requests': total - blocked,
            'block_rate': (blocked / total * 100) if total > 0 else 0,
            'adaptive_adjustments': self.metrics['adaptive_adjustments'],
            'active_users': len(self.adaptive_limiter.user_behavior_scores),
            'system_load_factor': self.adaptive_limiter.system_load_factor,
            'cache_entries': len(self.local_state)
        }
    
    async def _publish_rate_limit_event(self, client_id: str, endpoint: str, allowed: bool, 
                                      current_count: int, limit: int):
        """Publish rate limit event to Kafka for monitoring."""
        try:
            self.kafka.produce_rate_limit_event(
                client_id=client_id,
                endpoint=endpoint,
                allowed=allowed,
                current_count=current_count,
                limit=limit
            )
        except Exception as e:
            # Log error but don't fail the rate limiting
            import logging
            logging.error(f"Failed to publish rate limit event: {e}")

    def get_user_tier(self, user_id: str) -> RateLimitTier:
        """Get rate limit tier for user (implement based on user subscription/role)."""
        # Default implementation - should be customized based on business logic
        if user_id.startswith('enterprise_'):
            return self.rate_limit_tiers['enterprise']
        elif user_id.startswith('premium_'):
            return self.rate_limit_tiers['premium']
        else:
            return self.rate_limit_tiers['basic']
    
    async def is_allowed(
        self,
        key: str,
        limit: int,
        window: int,
        burst_limit: int | None = None,
        user_id: str = None,
        endpoint: str = None
    ) -> tuple[bool, dict[str, Any]]:
        """
        Distributed sliding window rate limiter
        
        Args:
            key: Unique identifier for the rate limit (e.g., user ID, IP)
            limit: Number of requests allowed per window
            window: Time window in seconds
            burst_limit: Maximum burst requests allowed
        
        Returns:
            Tuple of (is_allowed, metadata)
        """
        current_time = time.time()
        window_start = current_time - window
        
        # Update metrics
        self.metrics['total_requests'] += 1
        
        # Apply adaptive rate limiting if user_id is provided
        if user_id:
            # Get user tier
            user_tier = self.get_user_tier(user_id)
            
            # Use tier limits if they're different from provided limits
            if limit == self.rate_limit_tiers['basic'].limit:
                limit = user_tier.limit
                burst_limit = user_tier.burst_limit
            
            # Apply adaptive adjustment
            original_limit = limit
            limit = self.adaptive_limiter.calculate_adaptive_limit(limit, user_id)
            
            if limit != original_limit:
                self.metrics['adaptive_adjustments'] += 1
                logger.debug(f"Adaptive limit adjustment for {user_id}: {original_limit} -> {limit}")

        # Get local state first for performance
        requests, is_fresh = self._get_local_state(key, window)
        
        # If local state is stale, we still use it but will sync with distributed state
        if not is_fresh and requests:
            # In a production system, you might want to fetch from RabbitMQ here
            # For this implementation, we'll rely on local state with distributed sync
            pass

        # Add current request
        requests.append(current_time)
        
        # Filter requests within the current window
        valid_requests = [req_time for req_time in requests if req_time > window_start]
        current_count = len(valid_requests)

        # Update local cache
        self._update_local_state(key, valid_requests)

        # Check burst limit first
        if burst_limit and current_count > burst_limit:
            self.metrics['blocked_requests'] += 1
            
            # Update user behavior score negatively for exceeding burst limit
            if user_id:
                self.adaptive_limiter.update_user_behavior(user_id, -0.3)
            
            await self._publish_rate_limit_event(
                client_id=key, 
                endpoint=endpoint or "unknown", 
                allowed=False, 
                current_count=current_count, 
                limit=burst_limit
            )
            
            return False, {
                "allowed": False,
                "current_requests": current_count,
                "limit": limit,
                "burst_limit": burst_limit,
                "window": window,
                "reset_time": int(current_time + window),
                "retry_after": int(window),
                "reason": "burst_limit_exceeded"
            }

        # Check regular limit
        is_allowed = current_count <= limit
        
        if not is_allowed:
            self.metrics['blocked_requests'] += 1
            
            # Update user behavior score negatively
            if user_id:
                self.adaptive_limiter.update_user_behavior(user_id, -0.1)
        else:
            # Update user behavior score positively for good behavior
            if user_id and current_count < limit * 0.8:  # Using less than 80% of limit
                self.adaptive_limiter.update_user_behavior(user_id, 0.05)
        
        # Publish state update to RabbitMQ for distributed coordination
        await self._publish_state_update(key, valid_requests, window)

        # Publish monitoring event to Kafka
        await self._publish_rate_limit_event(
            client_id=key,
            endpoint=endpoint or "unknown",
            allowed=is_allowed,
            current_count=current_count,
            limit=limit
        )

        metadata = {
            "allowed": is_allowed,
            "current_requests": current_count,
            "limit": limit,
            "window": window,
            "reset_time": int(current_time + window),
            "remaining": max(0, limit - current_count),
            "retry_after": int(window) if not is_allowed else None,
            "reason": "rate_limit_exceeded" if not is_allowed else "allowed"
        }
        
        if burst_limit:
            metadata["burst_limit"] = burst_limit
            
        if user_id:
            metadata["user_tier"] = self.get_user_tier(user_id).name
            metadata["behavior_score"] = round(self.adaptive_limiter.user_behavior_scores.get(user_id, 1.0), 2)
            
        # Add system metrics
        metadata["system_load_factor"] = self.adaptive_limiter.system_load_factor

        return is_allowed, metadata


class RateLimitMiddleware(BaseHTTPMiddleware):
    """Rate limiting middleware using RabbitMQ for distributed state management."""
    
    def __init__(
        self,
        app,
        default_limit: int = 100,
        default_window: int = 60,
        burst_limit: int | None = None,
        rate_limit_rules: dict[str, dict[str, Any]] | None = None
    ):
        super().__init__(app)
        self.default_limit = default_limit
        self.default_window = default_window
        self.burst_limit = burst_limit
        self.rate_limit_rules = rate_limit_rules or {}
        
        # Initialize messaging systems
        self.rabbitmq_manager = RabbitMQManager()
        self.kafka_manager = KafkaManager()
        self.rate_limiter = DistributedSlidingWindowRateLimiter(
            self.rabbitmq_manager, 
            self.kafka_manager
        )

    def get_client_identifier(self, request: Request) -> tuple[str, str]:
        """Get unique client identifier and user ID for rate limiting.
        
        Returns:
            Tuple of (client_id, user_id)
        """
        user_id = None
        
        # Try to get user ID from JWT token
        auth_header = request.headers.get("authorization", "")
        if auth_header.startswith("Bearer "):
            try:
                # In a real implementation, decode the JWT to get user ID
                # For now, use a simple approach
                token_suffix = auth_header[-10:]
                user_id = f"user_{hash(token_suffix) % 10000}"
                return f"user:{user_id}", user_id
            except Exception:
                pass
        
        # Try to get API key from headers
        api_key = request.headers.get("x-api-key")
        if api_key:
            user_id = f"api_{hash(api_key) % 10000}"
            return f"api:{user_id}", user_id

        # Fallback to IP address
        client_ip = request.client.host
        x_forwarded_for = request.headers.get("x-forwarded-for")
        if x_forwarded_for:
            client_ip = x_forwarded_for.split(",")[0].strip()

        return f"ip:{client_ip}", None

    def get_rate_limit_config(self, request: Request) -> tuple[int, int, int | None]:
        """Get rate limit configuration for specific route"""
        route_key = f"{request.method}:{request.url.path}"

        # Check for specific route configuration
        if route_key in self.rate_limit_rules:
            config = self.rate_limit_rules[route_key]
            return (
                config.get("limit", self.default_limit),
                config.get("window", self.default_window),
                config.get("burst_limit", self.burst_limit)
            )

        # Check for path pattern configuration
        for pattern, config in self.rate_limit_rules.items():
            if request.url.path.startswith(pattern.replace("*", "")):
                return (
                    config.get("limit", self.default_limit),
                    config.get("window", self.default_window),
                    config.get("burst_limit", self.burst_limit)
                )

        return self.default_limit, self.default_window, self.burst_limit

    async def dispatch(self, request: Request, call_next) -> Response:
        start_time = time.time()
        
        # Skip rate limiting for health checks
        if request.url.path in ["/health", "/api/v1/health"]:
            return await call_next(request)

        client_id, user_id = self.get_client_identifier(request)
        limit, window, burst_limit = self.get_rate_limit_config(request)
        
        endpoint = f"{request.method} {request.url.path}"
        rate_limit_key = f"rate_limit:{client_id}:{request.url.path}"

        try:
            is_allowed, metadata = await self.rate_limiter.is_allowed(
                rate_limit_key, limit, window, burst_limit, user_id, endpoint
            )

            if not is_allowed:
                retry_after = metadata.get("retry_after", window)
                
                # Log rate limit exceeded with context
                logger.warning(
                    f"Rate limit exceeded for {client_id} on {endpoint}",
                    extra={
                        "client_id": client_id,
                        "user_id": user_id,
                        "endpoint": endpoint,
                        "current_requests": metadata.get("current_requests", 0),
                        "limit": limit,
                        "reason": metadata.get("reason", "unknown")
                    }
                )
                
                raise RateLimitExceeded(
                    detail=f"Rate limit exceeded. Try again in {retry_after} seconds.",
                    retry_after=retry_after
                )

            response = await call_next(request)

            # Add comprehensive rate limit headers
            response.headers["X-RateLimit-Limit"] = str(limit)
            response.headers["X-RateLimit-Remaining"] = str(metadata.get("remaining", 0))
            response.headers["X-RateLimit-Reset"] = str(metadata.get("reset_time", 0))
            response.headers["X-RateLimit-Window"] = str(window)
            
            if burst_limit:
                response.headers["X-RateLimit-Burst"] = str(burst_limit)
                
            if user_id:
                response.headers["X-RateLimit-Tier"] = metadata.get("user_tier", "basic")
                response.headers["X-RateLimit-Behavior-Score"] = str(metadata.get("behavior_score", 1.0))
            
            # Add performance metrics
            processing_time = (time.time() - start_time) * 1000
            response.headers["X-RateLimit-Processing-Time"] = f"{processing_time:.2f}ms"

            return response

        except RateLimitExceeded as e:
            response = Response(
                content=json.dumps({
                    "error": e.detail,
                    "retry_after": e.retry_after,
                    "timestamp": datetime.now().isoformat()
                }),
                status_code=e.status_code,
                headers={"Content-Type": "application/json"}
            )
            if e.retry_after:
                response.headers["Retry-After"] = str(e.retry_after)
            return response
            
        except Exception as e:
            # If distributed systems are down, allow requests but log the error
            import logging
            logging.error(f"Rate limiting error: {e}")
            
            # Still publish to Kafka if possible for monitoring
            try:
                await self.rate_limiter._publish_rate_limit_event(
                    client_id=client_id,
                    endpoint=endpoint,
                    allowed=True,  # Allowing due to system error
                    current_count=0,
                    limit=limit
                )
            except:
                pass
                
            # Log the error with context
            logger.error(
                f"Rate limiting system error for {client_id} on {endpoint}: {e}",
                extra={
                    "client_id": client_id,
                    "user_id": user_id,
                    "endpoint": endpoint,
                    "error": str(e)
                }
            )
                
            return await call_next(request)


class RateLimitStateManager:
    """Manages rate limit state synchronization across distributed instances."""
    
    def __init__(self):
        self.rabbitmq_manager = RabbitMQManager()
        self.kafka_manager = KafkaManager()
        
    def start_state_consumer(self):
        """Start consuming rate limit state updates from RabbitMQ."""
        
        def handle_state_update(state_message: StateMessage):
            """Handle incoming state update message."""
            if state_message.component == "rate_limiter":
                # In a full implementation, this would update a shared cache
                # or database with the distributed state
                print(f"Received rate limit state update: {state_message.key}")
                
                # Publish state change event to Kafka for monitoring
                self.kafka_manager.produce_state_change_event(
                    component="rate_limiter",
                    key=state_message.key,
                    operation="update",
                    metadata=state_message.metadata
                )
        
        # Start consuming state updates
        self.rabbitmq_manager.consume_state_updates(
            callback=handle_state_update,
            auto_ack=True
        )


# Factory function to create rate limit middleware with common configurations
def create_rate_limit_middleware(
    app,
    api_limits: dict[str, dict[str, Any]] | None = None
) -> RateLimitMiddleware:
    """Create rate limit middleware with common API endpoint configurations."""
    
    default_rules = {
        # Authentication endpoints - stricter limits
        "POST:/api/v1/auth/login": {"limit": 5, "window": 300, "burst_limit": 10},  # 5 per 5 min
        "POST:/api/v1/auth/register": {"limit": 3, "window": 3600, "burst_limit": 5},  # 3 per hour
        
        # API endpoints - moderate limits  
        "GET:/api/v1/*": {"limit": 1000, "window": 60, "burst_limit": 1500},  # 1000 per minute
        "POST:/api/v1/*": {"limit": 100, "window": 60, "burst_limit": 200},   # 100 per minute
        "PUT:/api/v1/*": {"limit": 100, "window": 60, "burst_limit": 150},    # 100 per minute
        "DELETE:/api/v1/*": {"limit": 50, "window": 60, "burst_limit": 100},  # 50 per minute
        
        # Heavy operations - strict limits
        "/api/v1/tasks/*": {"limit": 10, "window": 60, "burst_limit": 20},    # 10 per minute
        "/api/v1/reports/*": {"limit": 5, "window": 300, "burst_limit": 10},  # 5 per 5 minutes
        "/api/v1/analytics/*": {"limit": 20, "window": 60, "burst_limit": 40}, # 20 per minute
    }
    
    # Merge with custom limits if provided
    if api_limits:
        default_rules.update(api_limits)
    
    return RateLimitMiddleware(
        app=app,
        default_limit=500,
        default_window=60,
        burst_limit=750,
        rate_limit_rules=default_rules
    )


# Example usage and testing
if __name__ == "__main__":
    import asyncio
    
    async def test_rate_limiter():
        """Test the distributed rate limiter."""
        rabbitmq = RabbitMQManager()
        kafka = KafkaManager()
        
        rate_limiter = DistributedSlidingWindowRateLimiter(rabbitmq, kafka)
        
        # Test rate limiting
        for i in range(15):
            allowed, metadata = await rate_limiter.is_allowed(
                key="test_client",
                limit=10,
                window=60,
                burst_limit=12
            )
            
            print(f"Request {i+1}: Allowed={allowed}, Current={metadata['current_requests']}")
            
            if not allowed:
                print(f"Rate limit exceeded! Retry after {metadata.get('retry_after')} seconds")
                break
                
            await asyncio.sleep(0.1)  # Small delay between requests
    
    # Run the test
    asyncio.run(test_rate_limiter())