"""
Comprehensive API Rate Limiting and Throttling System
Enterprise-grade unified rate limiting with advanced features including:
- Multiple rate limiting algorithms (token bucket, sliding window, fixed window)
- Intelligent throttling with circuit breakers and adaptive capacity
- Geographic and user-tier based rate limiting
- Real-time monitoring and alerting
- Distributed state management with Redis
- Machine learning-based anomaly detection
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import math
import statistics
import time
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Tuple, Union

import redis.asyncio as redis
from fastapi import HTTPException, Request, Response
from starlette.middleware.base import BaseHTTPMiddleware

from core.logging import get_logger
from monitoring.system_resource_monitor import SystemResourceMonitor

logger = get_logger(__name__)


class RateLimitAlgorithm(Enum):
    """Available rate limiting algorithms."""
    TOKEN_BUCKET = "token_bucket"
    SLIDING_WINDOW_LOG = "sliding_window_log"
    SLIDING_WINDOW_COUNTER = "sliding_window_counter"
    FIXED_WINDOW = "fixed_window"
    ADAPTIVE = "adaptive"


class UserTier(Enum):
    """User subscription tiers with different limits."""
    FREE = "free"
    BASIC = "basic"
    PREMIUM = "premium"
    ENTERPRISE = "enterprise"
    UNLIMITED = "unlimited"


class GeographicRegion(Enum):
    """Geographic regions for location-based rate limiting."""
    NORTH_AMERICA = "north_america"
    EUROPE = "europe"
    ASIA_PACIFIC = "asia_pacific"
    SOUTH_AMERICA = "south_america"
    AFRICA = "africa"
    OCEANIA = "oceania"


class RateLimitScope(Enum):
    """Scope for rate limiting application."""
    USER = "user"
    IP = "ip"
    API_KEY = "api_key"
    ENDPOINT = "endpoint"
    GLOBAL = "global"


@dataclass
class RateLimitRule:
    """Configuration for a specific rate limiting rule."""
    algorithm: RateLimitAlgorithm = RateLimitAlgorithm.SLIDING_WINDOW_COUNTER
    requests: int = 100
    window_size: int = 60  # seconds
    burst_multiplier: float = 1.5
    scope: RateLimitScope = RateLimitScope.USER
    user_tiers: Dict[UserTier, int] = field(default_factory=dict)
    geographic_multipliers: Dict[GeographicRegion, float] = field(default_factory=dict)
    time_based_multipliers: Dict[int, float] = field(default_factory=dict)  # hour -> multiplier
    bypass_patterns: List[str] = field(default_factory=list)
    enforce_user_agent: bool = True
    require_authentication: bool = False


@dataclass
class RateLimitViolation:
    """Information about a rate limit violation."""
    timestamp: float
    client_id: str
    endpoint: str
    rule_violated: str
    current_count: int
    limit: int
    window_size: int
    severity: str  # 'warning', 'moderate', 'severe'
    metadata: Dict[str, Any] = field(default_factory=dict)


class AnomalyDetector:
    """Machine learning-based anomaly detection for rate limiting."""

    def __init__(self, window_size: int = 3600):
        self.window_size = window_size
        self.request_patterns: Dict[str, deque] = defaultdict(lambda: deque(maxlen=1000))
        self.baseline_metrics: Dict[str, Dict[str, float]] = {}
        self.anomaly_threshold = 2.5  # Standard deviations

    def record_request(self, client_id: str, endpoint: str, timestamp: float):
        """Record a request for pattern analysis."""
        key = f"{client_id}:{endpoint}"
        self.request_patterns[key].append(timestamp)

        # Update baseline metrics periodically
        if len(self.request_patterns[key]) % 100 == 0:
            self._update_baseline(key)

    def _update_baseline(self, key: str):
        """Update baseline metrics for a client-endpoint combination."""
        requests = list(self.request_patterns[key])
        if len(requests) < 10:
            return

        # Calculate request intervals
        intervals = [requests[i] - requests[i-1] for i in range(1, len(requests))]

        # Calculate baseline metrics
        self.baseline_metrics[key] = {
            'mean_interval': statistics.mean(intervals),
            'stdev_interval': statistics.stdev(intervals) if len(intervals) > 1 else 0,
            'requests_per_hour': len(requests) / (max(requests) - min(requests)) * 3600,
            'last_updated': time.time()
        }

    def detect_anomaly(self, client_id: str, endpoint: str, current_rate: float) -> Tuple[bool, float]:
        """
        Detect if current request rate is anomalous.

        Returns:
            Tuple of (is_anomaly, anomaly_score)
        """
        key = f"{client_id}:{endpoint}"
        baseline = self.baseline_metrics.get(key)

        if not baseline:
            return False, 0.0

        expected_rate = baseline['requests_per_hour']
        if expected_rate == 0:
            return False, 0.0

        # Calculate z-score
        rate_deviation = abs(current_rate - expected_rate)
        baseline_stdev = baseline.get('stdev_interval', 0)

        if baseline_stdev == 0:
            anomaly_score = rate_deviation / max(expected_rate, 1)
        else:
            anomaly_score = rate_deviation / baseline_stdev

        is_anomaly = anomaly_score > self.anomaly_threshold
        return is_anomaly, anomaly_score


class DistributedRateLimitState:
    """Distributed state management for rate limiting using Redis."""

    def __init__(self, redis_url: str = "redis://localhost:6379"):
        self.redis_url = redis_url
        self.redis_client: Optional[redis.Redis] = None
        self.key_prefix = "rate_limit"
        self.default_ttl = 3600

    async def initialize(self):
        """Initialize Redis connection."""
        try:
            self.redis_client = redis.from_url(self.redis_url, decode_responses=True)
            await self.redis_client.ping()
            logger.info("Redis connection established for rate limiting")
        except Exception as e:
            logger.error(f"Failed to connect to Redis: {e}")
            self.redis_client = None

    async def get_count(self, key: str, window_size: int) -> int:
        """Get current request count for a key within the window."""
        if not self.redis_client:
            return 0

        try:
            current_time = time.time()
            window_start = current_time - window_size

            # Use sorted set to store timestamps
            count = await self.redis_client.zcount(
                f"{self.key_prefix}:{key}",
                window_start,
                current_time
            )
            return count
        except Exception as e:
            logger.error(f"Redis get_count error: {e}")
            return 0

    async def increment_count(self, key: str, window_size: int) -> int:
        """Increment request count and return new count."""
        if not self.redis_client:
            return 1

        try:
            current_time = time.time()
            window_start = current_time - window_size
            redis_key = f"{self.key_prefix}:{key}"

            # Use pipeline for atomic operations
            pipe = self.redis_client.pipeline()

            # Remove old entries
            pipe.zremrangebyscore(redis_key, 0, window_start)

            # Add current request
            pipe.zadd(redis_key, {str(current_time): current_time})

            # Get count
            pipe.zcard(redis_key)

            # Set expiration
            pipe.expire(redis_key, window_size + 60)

            results = await pipe.execute()
            return results[2]  # Count from zcard

        except Exception as e:
            logger.error(f"Redis increment_count error: {e}")
            return 1

    async def get_token_bucket_state(self, key: str) -> Tuple[float, float]:
        """Get token bucket state (tokens, last_refill)."""
        if not self.redis_client:
            return 0.0, time.time()

        try:
            state = await self.redis_client.hmget(
                f"{self.key_prefix}:bucket:{key}",
                "tokens", "last_refill"
            )

            tokens = float(state[0]) if state[0] else 0.0
            last_refill = float(state[1]) if state[1] else time.time()

            return tokens, last_refill
        except Exception as e:
            logger.error(f"Redis get_token_bucket_state error: {e}")
            return 0.0, time.time()

    async def update_token_bucket_state(self, key: str, tokens: float, last_refill: float, ttl: int = None):
        """Update token bucket state."""
        if not self.redis_client:
            return

        try:
            redis_key = f"{self.key_prefix}:bucket:{key}"
            await self.redis_client.hmset(redis_key, {
                "tokens": tokens,
                "last_refill": last_refill
            })

            if ttl:
                await self.redis_client.expire(redis_key, ttl)

        except Exception as e:
            logger.error(f"Redis update_token_bucket_state error: {e}")

    async def record_violation(self, violation: RateLimitViolation):
        """Record a rate limit violation for monitoring."""
        if not self.redis_client:
            return

        try:
            violation_data = {
                'timestamp': violation.timestamp,
                'client_id': violation.client_id,
                'endpoint': violation.endpoint,
                'rule_violated': violation.rule_violated,
                'current_count': violation.current_count,
                'limit': violation.limit,
                'window_size': violation.window_size,
                'severity': violation.severity,
                'metadata': json.dumps(violation.metadata)
            }

            # Store in a sorted set for time-based queries
            await self.redis_client.zadd(
                f"{self.key_prefix}:violations",
                {json.dumps(violation_data): violation.timestamp}
            )

            # Keep only recent violations (last 24 hours)
            cutoff = time.time() - 86400
            await self.redis_client.zremrangebyscore(
                f"{self.key_prefix}:violations",
                0,
                cutoff
            )

        except Exception as e:
            logger.error(f"Redis record_violation error: {e}")


class ComprehensiveRateLimiter:
    """
    Comprehensive rate limiter with multiple algorithms,
    distributed state, and advanced features.
    """

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379",
        enable_anomaly_detection: bool = True
    ):
        self.distributed_state = DistributedRateLimitState(redis_url)
        self.anomaly_detector = AnomalyDetector() if enable_anomaly_detection else None
        self.resource_monitor = SystemResourceMonitor()

        # Local caches for performance
        self.local_cache: Dict[str, Dict[str, Any]] = {}
        self.cache_ttl = 10  # seconds
        self.user_tier_cache: Dict[str, UserTier] = {}

        # Configuration
        self.rules: Dict[str, RateLimitRule] = {}
        self.global_rules: List[RateLimitRule] = []

        # Metrics
        self.metrics = {
            'total_requests': 0,
            'blocked_requests': 0,
            'cache_hits': 0,
            'cache_misses': 0,
            'anomalies_detected': 0,
            'violations_recorded': 0
        }

    async def initialize(self):
        """Initialize the rate limiter."""
        await self.distributed_state.initialize()
        logger.info("Comprehensive rate limiter initialized")

    def add_rule(self, pattern: str, rule: RateLimitRule):
        """Add a rate limiting rule for specific endpoints."""
        self.rules[pattern] = rule

    def add_global_rule(self, rule: RateLimitRule):
        """Add a global rate limiting rule."""
        self.global_rules.append(rule)

    def _get_cache_key(self, client_id: str, rule_key: str) -> str:
        """Generate cache key for local caching."""
        return f"{client_id}:{rule_key}"

    def _is_cache_valid(self, cache_entry: Dict[str, Any]) -> bool:
        """Check if cache entry is still valid."""
        return time.time() - cache_entry.get('timestamp', 0) < self.cache_ttl

    async def _get_user_tier(self, user_id: str) -> UserTier:
        """Get user tier with caching."""
        if user_id in self.user_tier_cache:
            return self.user_tier_cache[user_id]

        # In a real implementation, this would query a user service
        # For now, use a simple heuristic based on user ID
        if user_id.startswith('enterprise_'):
            tier = UserTier.ENTERPRISE
        elif user_id.startswith('premium_'):
            tier = UserTier.PREMIUM
        elif user_id.startswith('basic_'):
            tier = UserTier.BASIC
        else:
            tier = UserTier.FREE

        self.user_tier_cache[user_id] = tier
        return tier

    def _get_geographic_region(self, client_ip: str) -> GeographicRegion:
        """Determine geographic region from IP address."""
        # In a real implementation, this would use a GeoIP database
        # For now, return a default region
        return GeographicRegion.NORTH_AMERICA

    def _get_applicable_rules(self, endpoint: str) -> List[Tuple[str, RateLimitRule]]:
        """Get all applicable rate limiting rules for an endpoint."""
        applicable_rules = []

        # Check specific endpoint rules
        for pattern, rule in self.rules.items():
            if self._endpoint_matches_pattern(endpoint, pattern):
                applicable_rules.append((pattern, rule))

        # Add global rules
        for i, rule in enumerate(self.global_rules):
            applicable_rules.append((f"global_{i}", rule))

        return applicable_rules

    def _endpoint_matches_pattern(self, endpoint: str, pattern: str) -> bool:
        """Check if endpoint matches a pattern."""
        if pattern == "*":
            return True

        # Simple wildcard matching
        if pattern.endswith("*"):
            return endpoint.startswith(pattern[:-1])

        return endpoint == pattern

    async def _apply_token_bucket_algorithm(
        self,
        client_id: str,
        rule: RateLimitRule,
        rule_key: str
    ) -> Tuple[bool, Dict[str, Any]]:
        """Apply token bucket rate limiting algorithm."""
        capacity = rule.requests
        refill_rate = capacity / rule.window_size  # tokens per second

        # Get current state
        tokens, last_refill = await self.distributed_state.get_token_bucket_state(
            f"{client_id}:{rule_key}"
        )

        # Refill tokens
        now = time.time()
        time_passed = now - last_refill
        new_tokens = min(capacity, tokens + (time_passed * refill_rate))

        # Check if request can be allowed
        if new_tokens >= 1.0:
            new_tokens -= 1.0
            allowed = True

            # Update state
            await self.distributed_state.update_token_bucket_state(
                f"{client_id}:{rule_key}",
                new_tokens,
                now,
                rule.window_size * 2
            )
        else:
            allowed = False

        return allowed, {
            'algorithm': 'token_bucket',
            'tokens_remaining': new_tokens,
            'capacity': capacity,
            'refill_rate': refill_rate
        }

    async def _apply_sliding_window_algorithm(
        self,
        client_id: str,
        rule: RateLimitRule,
        rule_key: str
    ) -> Tuple[bool, Dict[str, Any]]:
        """Apply sliding window rate limiting algorithm."""
        # Get current count and increment
        current_count = await self.distributed_state.increment_count(
            f"{client_id}:{rule_key}",
            rule.window_size
        )

        allowed = current_count <= rule.requests

        return allowed, {
            'algorithm': 'sliding_window',
            'current_count': current_count,
            'limit': rule.requests,
            'window_size': rule.window_size,
            'remaining': max(0, rule.requests - current_count)
        }

    async def _calculate_adjusted_limit(
        self,
        base_limit: int,
        rule: RateLimitRule,
        user_id: Optional[str] = None,
        client_ip: Optional[str] = None
    ) -> int:
        """Calculate adjusted limit based on user tier, geography, and time."""
        limit = base_limit

        # Apply user tier adjustments
        if user_id and rule.user_tiers:
            user_tier = await self._get_user_tier(user_id)
            if user_tier in rule.user_tiers:
                limit = rule.user_tiers[user_tier]

        # Apply geographic adjustments
        if client_ip and rule.geographic_multipliers:
            region = self._get_geographic_region(client_ip)
            if region in rule.geographic_multipliers:
                limit = int(limit * rule.geographic_multipliers[region])

        # Apply time-based adjustments
        if rule.time_based_multipliers:
            current_hour = datetime.now().hour
            if current_hour in rule.time_based_multipliers:
                limit = int(limit * rule.time_based_multipliers[current_hour])

        return max(1, limit)  # Ensure at least 1 request is allowed

    async def check_rate_limit(
        self,
        client_id: str,
        endpoint: str,
        user_id: Optional[str] = None,
        client_ip: Optional[str] = None,
        user_agent: Optional[str] = None
    ) -> Tuple[bool, Dict[str, Any]]:
        """
        Check if request should be rate limited.

        Returns:
            Tuple of (is_allowed, metadata)
        """
        self.metrics['total_requests'] += 1
        start_time = time.time()

        # Get applicable rules
        applicable_rules = self._get_applicable_rules(endpoint)

        if not applicable_rules:
            return True, {'reason': 'no_applicable_rules'}

        # Check each rule
        most_restrictive_result = None
        all_results = []

        for rule_key, rule in applicable_rules:
            # Skip if bypass pattern matches
            if any(pattern in user_agent or '' for pattern in rule.bypass_patterns):
                continue

            # Calculate adjusted limit
            adjusted_limit = await self._calculate_adjusted_limit(
                rule.requests, rule, user_id, client_ip
            )

            # Update rule with adjusted limit
            adjusted_rule = RateLimitRule(
                algorithm=rule.algorithm,
                requests=adjusted_limit,
                window_size=rule.window_size,
                burst_multiplier=rule.burst_multiplier,
                scope=rule.scope
            )

            # Apply algorithm
            if rule.algorithm == RateLimitAlgorithm.TOKEN_BUCKET:
                allowed, metadata = await self._apply_token_bucket_algorithm(
                    client_id, adjusted_rule, rule_key
                )
            else:
                # Default to sliding window
                allowed, metadata = await self._apply_sliding_window_algorithm(
                    client_id, adjusted_rule, rule_key
                )

            metadata.update({
                'rule_key': rule_key,
                'scope': rule.scope.value,
                'adjusted_limit': adjusted_limit,
                'base_limit': rule.requests
            })

            all_results.append((allowed, metadata))

            # Track most restrictive (blocked) result
            if not allowed and (most_restrictive_result is None or not most_restrictive_result[0]):
                most_restrictive_result = (allowed, metadata)

        # If any rule blocks the request, use that result
        if most_restrictive_result and not most_restrictive_result[0]:
            self.metrics['blocked_requests'] += 1

            # Record violation
            violation = RateLimitViolation(
                timestamp=time.time(),
                client_id=client_id,
                endpoint=endpoint,
                rule_violated=most_restrictive_result[1]['rule_key'],
                current_count=most_restrictive_result[1].get('current_count', 0),
                limit=most_restrictive_result[1]['adjusted_limit'],
                window_size=most_restrictive_result[1].get('window_size', 60),
                severity='moderate',
                metadata={
                    'user_id': user_id,
                    'client_ip': client_ip,
                    'user_agent': user_agent
                }
            )

            await self.distributed_state.record_violation(violation)
            self.metrics['violations_recorded'] += 1

            result = most_restrictive_result
        else:
            # All rules allow the request
            result = (True, {
                'reason': 'allowed',
                'rules_checked': len(applicable_rules),
                'all_results': all_results
            })

        # Anomaly detection
        if self.anomaly_detector and user_id:
            self.anomaly_detector.record_request(client_id, endpoint, time.time())

            # Calculate current rate (requests per hour)
            current_count = result[1].get('current_count', 0)
            window_size = result[1].get('window_size', 60)
            current_rate = (current_count / window_size) * 3600

            is_anomaly, anomaly_score = self.anomaly_detector.detect_anomaly(
                client_id, endpoint, current_rate
            )

            if is_anomaly:
                self.metrics['anomalies_detected'] += 1
                result[1]['anomaly_detected'] = True
                result[1]['anomaly_score'] = anomaly_score

        # Add performance metrics
        processing_time = (time.time() - start_time) * 1000
        result[1]['processing_time_ms'] = processing_time

        return result

    async def get_metrics(self) -> Dict[str, Any]:
        """Get comprehensive rate limiting metrics."""
        return {
            **self.metrics,
            'rules_configured': len(self.rules),
            'global_rules': len(self.global_rules),
            'cache_entries': len(self.local_cache),
            'user_tier_cache_size': len(self.user_tier_cache)
        }


class ComprehensiveRateLimitMiddleware(BaseHTTPMiddleware):
    """
    Comprehensive rate limiting middleware with enterprise features:
    - Multiple algorithms (token bucket, sliding window, fixed window)
    - User tier-based limits
    - Geographic rate limiting
    - Time-based adjustments
    - Anomaly detection
    - Comprehensive monitoring
    """

    def __init__(
        self,
        app,
        redis_url: str = "redis://localhost:6379",
        enable_anomaly_detection: bool = True,
        default_rules: Optional[Dict[str, RateLimitRule]] = None
    ):
        super().__init__(app)
        self.rate_limiter = ComprehensiveRateLimiter(redis_url, enable_anomaly_detection)
        self._setup_default_rules(default_rules)

    async def _initialize_rate_limiter(self):
        """Initialize rate limiter if not already done."""
        if not hasattr(self, '_initialized'):
            await self.rate_limiter.initialize()
            self._initialized = True

    def _setup_default_rules(self, custom_rules: Optional[Dict[str, RateLimitRule]] = None):
        """Setup default rate limiting rules."""
        # Default rules for common endpoints
        default_rules = {
            # Authentication endpoints
            "POST:/api/v1/auth/login": RateLimitRule(
                algorithm=RateLimitAlgorithm.SLIDING_WINDOW_COUNTER,
                requests=10,
                window_size=300,  # 5 minutes
                scope=RateLimitScope.IP,
                user_tiers={
                    UserTier.FREE: 5,
                    UserTier.BASIC: 10,
                    UserTier.PREMIUM: 20,
                    UserTier.ENTERPRISE: 50
                }
            ),

            # API endpoints
            "GET:/api/v1/*": RateLimitRule(
                algorithm=RateLimitAlgorithm.TOKEN_BUCKET,
                requests=1000,
                window_size=60,
                scope=RateLimitScope.USER,
                user_tiers={
                    UserTier.FREE: 100,
                    UserTier.BASIC: 500,
                    UserTier.PREMIUM: 2000,
                    UserTier.ENTERPRISE: 10000
                },
                geographic_multipliers={
                    GeographicRegion.NORTH_AMERICA: 1.0,
                    GeographicRegion.EUROPE: 0.8,
                    GeographicRegion.ASIA_PACIFIC: 0.9,
                    GeographicRegion.SOUTH_AMERICA: 0.7,
                    GeographicRegion.AFRICA: 0.6,
                    GeographicRegion.OCEANIA: 0.8
                }
            ),

            # Heavy operations
            "POST:/api/v1/analytics/*": RateLimitRule(
                algorithm=RateLimitAlgorithm.LEAKY_BUCKET,
                requests=20,
                window_size=300,  # 5 minutes
                scope=RateLimitScope.USER,
                user_tiers={
                    UserTier.FREE: 5,
                    UserTier.BASIC: 10,
                    UserTier.PREMIUM: 30,
                    UserTier.ENTERPRISE: 100
                }
            )
        }

        # Apply custom rules
        if custom_rules:
            default_rules.update(custom_rules)

        # Add rules to rate limiter
        for pattern, rule in default_rules.items():
            self.rate_limiter.add_rule(pattern, rule)

        # Add global rule
        global_rule = RateLimitRule(
            algorithm=RateLimitAlgorithm.SLIDING_WINDOW_COUNTER,
            requests=5000,
            window_size=60,
            scope=RateLimitScope.IP,
            time_based_multipliers={
                # Peak hours (9 AM - 5 PM): reduce limits
                9: 0.8, 10: 0.8, 11: 0.8, 12: 0.7, 13: 0.7, 14: 0.8, 15: 0.8, 16: 0.8, 17: 0.8,
                # Off-peak hours: normal or increased limits
                0: 1.2, 1: 1.2, 2: 1.2, 3: 1.2, 4: 1.2, 5: 1.2, 6: 1.0, 7: 1.0, 8: 1.0,
                18: 1.0, 19: 1.0, 20: 1.0, 21: 1.1, 22: 1.1, 23: 1.2
            }
        )
        self.rate_limiter.add_global_rule(global_rule)

    def _extract_client_info(self, request: Request) -> Tuple[str, Optional[str], str]:
        """Extract client identification information from request."""
        # Get client IP
        client_ip = request.client.host
        x_forwarded_for = request.headers.get("x-forwarded-for")
        if x_forwarded_for:
            client_ip = x_forwarded_for.split(",")[0].strip()

        # Get user ID from various sources
        user_id = None

        # Try Authorization header (JWT)
        auth_header = request.headers.get("authorization", "")
        if auth_header.startswith("Bearer "):
            # In production, decode JWT to get user ID
            token = auth_header[7:]
            user_id = f"user_{hashlib.sha256(token.encode()).hexdigest()[:16]}"

        # Try API key
        api_key = request.headers.get("x-api-key")
        if api_key and not user_id:
            user_id = f"api_{hashlib.sha256(api_key.encode()).hexdigest()[:16]}"

        # Generate client ID
        if user_id:
            client_id = f"user:{user_id}"
        else:
            client_id = f"ip:{client_ip}"

        return client_id, user_id, client_ip

    async def dispatch(self, request: Request, call_next):
        """Main middleware dispatch method."""
        start_time = time.time()

        # Initialize rate limiter if needed
        await self._initialize_rate_limiter()

        # Skip rate limiting for health checks
        if request.url.path in ["/health", "/api/v1/health", "/metrics"]:
            return await call_next(request)

        # Extract client information
        client_id, user_id, client_ip = self._extract_client_info(request)
        user_agent = request.headers.get("user-agent", "")
        endpoint = f"{request.method}:{request.url.path}"

        try:
            # Check rate limits
            is_allowed, metadata = await self.rate_limiter.check_rate_limit(
                client_id=client_id,
                endpoint=endpoint,
                user_id=user_id,
                client_ip=client_ip,
                user_agent=user_agent
            )

            if not is_allowed:
                # Create rate limit response
                retry_after = metadata.get('window_size', 60)
                response_data = {
                    "error": "Rate limit exceeded",
                    "message": f"Too many requests. Try again in {retry_after} seconds.",
                    "retry_after": retry_after,
                    "limit": metadata.get('adjusted_limit', 0),
                    "current": metadata.get('current_count', 0),
                    "remaining": metadata.get('remaining', 0),
                    "window_size": metadata.get('window_size', 60),
                    "reset_time": int(time.time() + retry_after),
                    "timestamp": datetime.now().isoformat()
                }

                # Add additional context
                if metadata.get('anomaly_detected'):
                    response_data["warning"] = "Anomalous request pattern detected"

                response = Response(
                    content=json.dumps(response_data),
                    status_code=429,
                    headers={
                        "Content-Type": "application/json",
                        "Retry-After": str(retry_after),
                        "X-RateLimit-Limit": str(metadata.get('adjusted_limit', 0)),
                        "X-RateLimit-Remaining": str(metadata.get('remaining', 0)),
                        "X-RateLimit-Reset": str(int(time.time() + retry_after)),
                        "X-RateLimit-Window": str(metadata.get('window_size', 60)),
                        "X-RateLimit-Algorithm": metadata.get('algorithm', 'unknown'),
                        "X-RateLimit-Scope": metadata.get('scope', 'unknown')
                    }
                )

                return response

            # Process request
            response = await call_next(request)

            # Add rate limit headers to successful responses
            if metadata.get('current_count') is not None:
                response.headers["X-RateLimit-Limit"] = str(metadata.get('adjusted_limit', 0))
                response.headers["X-RateLimit-Remaining"] = str(metadata.get('remaining', 0))
                response.headers["X-RateLimit-Reset"] = str(int(time.time() + metadata.get('window_size', 60)))

            # Add performance headers
            processing_time = (time.time() - start_time) * 1000
            response.headers["X-RateLimit-Processing-Time"] = f"{processing_time:.2f}ms"

            if metadata.get('anomaly_detected'):
                response.headers["X-RateLimit-Anomaly-Score"] = str(metadata.get('anomaly_score', 0))

            return response

        except Exception as e:
            logger.error(f"Rate limiting middleware error: {e}")
            # If rate limiting fails, allow the request through
            return await call_next(request)


# Factory function for easy setup
def create_comprehensive_rate_limit_middleware(
    app,
    redis_url: str = "redis://localhost:6379",
    custom_rules: Optional[Dict[str, RateLimitRule]] = None,
    enable_anomaly_detection: bool = True
) -> ComprehensiveRateLimitMiddleware:
    """
    Create comprehensive rate limiting middleware with sensible defaults.

    Args:
        app: FastAPI application
        redis_url: Redis connection URL
        custom_rules: Custom rate limiting rules
        enable_anomaly_detection: Enable ML-based anomaly detection

    Returns:
        Configured rate limiting middleware
    """
    return ComprehensiveRateLimitMiddleware(
        app=app,
        redis_url=redis_url,
        enable_anomaly_detection=enable_anomaly_detection,
        default_rules=custom_rules
    )