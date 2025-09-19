"""
GraphQL Query Complexity Analysis and Rate Limiting
Enterprise-grade GraphQL security with intelligent query analysis and adaptive rate limiting.
"""

import asyncio
import json
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union, Tuple, Set
from enum import Enum
from dataclasses import dataclass, field
from collections import defaultdict, deque
import hashlib
import re
from functools import lru_cache

from graphql import (
    GraphQLSchema, DocumentNode, validate, parse,
    ValidationRule, TypeInfo, visit, Visitor,
    FieldNode, FragmentDefinitionNode, InlineFragmentNode,
    SelectionSetNode, OperationType, OperationDefinitionNode
)
from graphql.validation import ValidationContext
from graphql.type import is_list_type, is_non_null_type, get_named_type
import redis
from fastapi import HTTPException, status

from core.config.base_config import BaseConfig
from core.logging import get_logger

logger = get_logger(__name__)


class ComplexityLevel(Enum):
    """Query complexity levels"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"
    BLOCKED = "blocked"


class RateLimitType(Enum):
    """Rate limiting types"""
    QUERY_COUNT = "query_count"
    COMPLEXITY_BUDGET = "complexity_budget"
    DEPTH_LIMIT = "depth_limit"
    FIELD_COUNT = "field_count"
    RESOLVER_CALLS = "resolver_calls"


class QueryType(Enum):
    """GraphQL query types"""
    QUERY = "query"
    MUTATION = "mutation"
    SUBSCRIPTION = "subscription"


@dataclass
class ComplexityScore:
    """Complexity scoring for GraphQL operations"""
    field_complexity: int
    depth_complexity: int
    list_complexity: int
    resolver_complexity: int
    custom_complexity: int
    total_complexity: int
    level: ComplexityLevel
    analysis_details: Dict[str, Any] = field(default_factory=dict)


@dataclass
class QueryMetrics:
    """Metrics for GraphQL query execution"""
    query_id: str
    operation_name: Optional[str]
    operation_type: QueryType
    complexity_score: ComplexityScore
    execution_time_ms: Optional[float]
    resolver_count: int
    field_count: int
    max_depth: int
    has_fragments: bool
    has_variables: bool
    timestamp: datetime
    client_id: str
    user_id: Optional[str]
    rate_limit_applied: bool
    blocked_reason: Optional[str]


@dataclass
class RateLimitRule:
    """Rate limiting rule configuration"""
    name: str
    limit_type: RateLimitType
    window_seconds: int
    max_requests: int
    complexity_threshold: int
    user_specific: bool
    burst_allowance: int
    penalty_duration_seconds: int
    priority: int
    conditions: Dict[str, Any] = field(default_factory=dict)


@dataclass
class RateLimitState:
    """Current rate limiting state for a client"""
    client_id: str
    window_start: datetime
    request_count: int
    complexity_used: int
    blocked_until: Optional[datetime]
    violations: List[Dict[str, Any]]
    last_request: datetime


class GraphQLQueryComplexityAnalyzer:
    """
    Enterprise GraphQL query complexity analyzer with adaptive rate limiting.

    Features:
    - Real-time query complexity analysis
    - Depth and breadth complexity calculation
    - Custom field complexity scoring
    - Intelligent rate limiting with burst allowance
    - Query pattern analysis and detection
    - Performance impact prediction
    - Security threat detection
    - Comprehensive audit logging
    """

    def __init__(self, schema: GraphQLSchema, config: Optional[Dict] = None):
        self.schema = schema
        self.config = config or {}
        self.logger = get_logger(f"{__name__}.{self.__class__.__name__}")

        # Configuration
        self.max_complexity = self.config.get('max_complexity', 1000)
        self.max_depth = self.config.get('max_depth', 15)
        self.default_field_complexity = self.config.get('default_field_complexity', 1)
        self.list_multiplier = self.config.get('list_multiplier', 10)
        self.enable_introspection_limit = self.config.get('enable_introspection_limit', True)

        # Rate limiting
        self.redis_client = None
        self.rate_limit_rules: List[RateLimitRule] = []
        self.rate_limit_states: Dict[str, RateLimitState] = {}

        # Query analysis
        self.query_metrics: Dict[str, QueryMetrics] = {}
        self.query_patterns: Dict[str, int] = defaultdict(int)
        self.malicious_patterns: Set[str] = set()

        # Field complexity mapping
        self.field_complexity_map: Dict[str, int] = {}
        self.resolver_complexity_map: Dict[str, int] = {}

        # Performance tracking
        self.performance_history: deque = deque(maxlen=1000)

        self._initialize_default_rules()
        self._initialize_complexity_maps()

    async def initialize(self):
        """Initialize the complexity analyzer"""
        try:
            # Initialize Redis connection
            redis_url = self.config.get('redis_url', 'redis://localhost:6379/1')
            self.redis_client = redis.from_url(redis_url, decode_responses=True)

            # Test Redis connection
            await self._test_redis_connection()

            self.logger.info("GraphQL Query Complexity Analyzer initialized successfully")

        except Exception as e:
            self.logger.error(f"Failed to initialize complexity analyzer: {e}")
            # Continue without Redis (in-memory fallback)
            self.logger.warning("Using in-memory rate limiting fallback")

    def _initialize_default_rules(self):
        """Initialize default rate limiting rules"""

        # Query count rate limiting
        self.rate_limit_rules.append(RateLimitRule(
            name="query_count_per_minute",
            limit_type=RateLimitType.QUERY_COUNT,
            window_seconds=60,
            max_requests=100,
            complexity_threshold=0,
            user_specific=True,
            burst_allowance=20,
            penalty_duration_seconds=300,  # 5 minutes
            priority=1
        ))

        # Complexity budget rate limiting
        self.rate_limit_rules.append(RateLimitRule(
            name="complexity_budget_per_minute",
            limit_type=RateLimitType.COMPLEXITY_BUDGET,
            window_seconds=60,
            max_requests=0,  # Not applicable for complexity budget
            complexity_threshold=5000,
            user_specific=True,
            burst_allowance=1000,
            penalty_duration_seconds=600,  # 10 minutes
            priority=2
        ))

        # High complexity query limiting
        self.rate_limit_rules.append(RateLimitRule(
            name="high_complexity_queries",
            limit_type=RateLimitType.COMPLEXITY_BUDGET,
            window_seconds=300,  # 5 minutes
            max_requests=10,
            complexity_threshold=500,
            user_specific=True,
            burst_allowance=2,
            penalty_duration_seconds=900,  # 15 minutes
            priority=3,
            conditions={"min_complexity": 500}
        ))

        # Depth limiting
        self.rate_limit_rules.append(RateLimitRule(
            name="excessive_depth_queries",
            limit_type=RateLimitType.DEPTH_LIMIT,
            window_seconds=60,
            max_requests=5,
            complexity_threshold=0,
            user_specific=True,
            burst_allowance=1,
            penalty_duration_seconds=1200,  # 20 minutes
            priority=4,
            conditions={"min_depth": 10}
        ))

    def _initialize_complexity_maps(self):
        """Initialize field and resolver complexity mappings"""

        # Field complexity mappings (higher values for expensive operations)
        self.field_complexity_map = {
            # Basic fields
            'id': 1,
            'name': 1,
            'email': 1,
            'createdAt': 1,
            'updatedAt': 1,

            # Expensive aggregation fields
            'analytics': 50,
            'aggregatedSales': 30,
            'customerAnalytics': 40,
            'revenueAnalysis': 35,
            'performanceMetrics': 25,

            # Relationship fields with potential N+1 issues
            'orders': 10,
            'customers': 15,
            'products': 8,
            'transactions': 12,
            'reviews': 5,

            # Search and filtering operations
            'search': 20,
            'filter': 15,
            'sort': 10,

            # Real-time or computed fields
            'currentStatus': 25,
            'liveMetrics': 30,
            'calculatedValue': 20,

            # Admin and sensitive operations
            'adminSettings': 100,
            'systemMetrics': 80,
            'auditLogs': 60,
            'securitySettings': 120,

            # Introspection queries
            '__schema': 50,
            '__type': 30,
            '__field': 20
        }

        # Resolver complexity mappings
        self.resolver_complexity_map = {
            'resolve_analytics': 50,
            'resolve_aggregated_data': 40,
            'resolve_complex_calculations': 60,
            'resolve_third_party_data': 80,
            'resolve_real_time_data': 70,
            'resolve_batch_operations': 90
        }

    async def _test_redis_connection(self):
        """Test Redis connection"""
        if self.redis_client:
            try:
                await asyncio.get_event_loop().run_in_executor(None, self.redis_client.ping)
                self.logger.debug("Redis connection successful")
            except Exception as e:
                self.logger.error(f"Redis connection failed: {e}")
                self.redis_client = None

    async def analyze_query_complexity(
        self,
        document: DocumentNode,
        variables: Optional[Dict[str, Any]] = None,
        client_id: str = "anonymous",
        user_id: Optional[str] = None,
        operation_name: Optional[str] = None
    ) -> QueryMetrics:
        """
        Analyze GraphQL query complexity and apply rate limiting

        Args:
            document: Parsed GraphQL document
            variables: Query variables
            client_id: Client identifier for rate limiting
            user_id: User identifier for user-specific limits
            operation_name: Optional operation name

        Returns:
            QueryMetrics: Comprehensive query analysis results
        """
        start_time = time.time()
        query_id = self._generate_query_id(document, variables)

        try:
            # Validate query structure
            validation_errors = validate(self.schema, document)
            if validation_errors:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"GraphQL validation errors: {[str(e) for e in validation_errors]}"
                )

            # Extract operation information
            operation = self._extract_operation(document, operation_name)
            operation_type = self._get_operation_type(operation)

            # Calculate complexity
            complexity_score = self._calculate_complexity(document, variables, operation)

            # Check for malicious patterns
            self._detect_malicious_patterns(document, complexity_score)

            # Apply rate limiting
            rate_limit_result = await self._apply_rate_limiting(
                client_id, user_id, complexity_score, operation_type
            )

            # Calculate execution metrics
            execution_time_ms = (time.time() - start_time) * 1000
            field_count = self._count_fields(document)
            max_depth = self._calculate_max_depth(document)

            # Create query metrics
            query_metrics = QueryMetrics(
                query_id=query_id,
                operation_name=operation_name,
                operation_type=operation_type,
                complexity_score=complexity_score,
                execution_time_ms=execution_time_ms,
                resolver_count=complexity_score.analysis_details.get('resolver_count', 0),
                field_count=field_count,
                max_depth=max_depth,
                has_fragments=self._has_fragments(document),
                has_variables=bool(variables),
                timestamp=datetime.utcnow(),
                client_id=client_id,
                user_id=user_id,
                rate_limit_applied=rate_limit_result['limited'],
                blocked_reason=rate_limit_result.get('reason')
            )

            # Store metrics
            self.query_metrics[query_id] = query_metrics
            self.performance_history.append({
                'timestamp': datetime.utcnow(),
                'complexity': complexity_score.total_complexity,
                'execution_time_ms': execution_time_ms,
                'client_id': client_id
            })

            # Log complex or blocked queries
            if complexity_score.level in [ComplexityLevel.HIGH, ComplexityLevel.CRITICAL]:
                self.logger.warning(f"High complexity query detected: {query_id}, complexity: {complexity_score.total_complexity}")

            if rate_limit_result['limited']:
                self.logger.info(f"Query rate limited: {query_id}, reason: {rate_limit_result.get('reason')}")
                raise HTTPException(
                    status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                    detail=f"Query rate limit exceeded: {rate_limit_result.get('reason')}",
                    headers={
                        "X-RateLimit-Reason": rate_limit_result.get('reason', 'rate_limited'),
                        "X-Query-Complexity": str(complexity_score.total_complexity),
                        "X-Retry-After": str(rate_limit_result.get('retry_after_seconds', 60))
                    }
                )

            return query_metrics

        except HTTPException:
            raise
        except Exception as e:
            self.logger.error(f"Query analysis failed for {query_id}: {e}")
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Query analysis failed"
            )

    def _calculate_complexity(
        self,
        document: DocumentNode,
        variables: Optional[Dict[str, Any]],
        operation: OperationDefinitionNode
    ) -> ComplexityScore:
        """Calculate comprehensive query complexity score"""

        complexity_visitor = ComplexityAnalysisVisitor(
            schema=self.schema,
            field_complexity_map=self.field_complexity_map,
            resolver_complexity_map=self.resolver_complexity_map,
            default_complexity=self.default_field_complexity,
            list_multiplier=self.list_multiplier,
            variables=variables or {}
        )

        # Visit the document to calculate complexity
        visit(document, complexity_visitor)

        # Calculate different complexity dimensions
        field_complexity = complexity_visitor.field_complexity
        depth_complexity = complexity_visitor.max_depth * 5  # Depth penalty
        list_complexity = complexity_visitor.list_complexity
        resolver_complexity = complexity_visitor.resolver_complexity
        custom_complexity = 0  # For custom business logic

        total_complexity = (
            field_complexity +
            depth_complexity +
            list_complexity +
            resolver_complexity +
            custom_complexity
        )

        # Determine complexity level
        if total_complexity >= self.max_complexity:
            level = ComplexityLevel.BLOCKED
        elif total_complexity >= self.max_complexity * 0.8:
            level = ComplexityLevel.CRITICAL
        elif total_complexity >= self.max_complexity * 0.6:
            level = ComplexityLevel.HIGH
        elif total_complexity >= self.max_complexity * 0.3:
            level = ComplexityLevel.MEDIUM
        else:
            level = ComplexityLevel.LOW

        return ComplexityScore(
            field_complexity=field_complexity,
            depth_complexity=depth_complexity,
            list_complexity=list_complexity,
            resolver_complexity=resolver_complexity,
            custom_complexity=custom_complexity,
            total_complexity=total_complexity,
            level=level,
            analysis_details={
                'max_depth': complexity_visitor.max_depth,
                'field_count': complexity_visitor.field_count,
                'list_field_count': complexity_visitor.list_field_count,
                'resolver_count': complexity_visitor.resolver_count,
                'introspection_fields': complexity_visitor.introspection_field_count,
                'expensive_fields': complexity_visitor.expensive_fields
            }
        )

    async def _apply_rate_limiting(
        self,
        client_id: str,
        user_id: Optional[str],
        complexity_score: ComplexityScore,
        operation_type: QueryType
    ) -> Dict[str, Any]:
        """Apply rate limiting rules"""

        rate_limit_key = f"{client_id}:{user_id}" if user_id else client_id

        # Check if client is currently blocked
        if rate_limit_key in self.rate_limit_states:
            state = self.rate_limit_states[rate_limit_key]
            if state.blocked_until and datetime.utcnow() < state.blocked_until:
                return {
                    'limited': True,
                    'reason': 'client_blocked',
                    'retry_after_seconds': int((state.blocked_until - datetime.utcnow()).total_seconds())
                }

        # Apply each rate limiting rule
        for rule in sorted(self.rate_limit_rules, key=lambda r: r.priority):
            limit_result = await self._check_rate_limit_rule(
                rule, rate_limit_key, complexity_score, operation_type
            )

            if limit_result['limited']:
                # Update client state
                await self._update_rate_limit_state(
                    rate_limit_key, rule, limit_result, complexity_score
                )
                return limit_result

        # Update successful request state
        await self._update_successful_request_state(rate_limit_key, complexity_score)

        return {'limited': False}

    async def _check_rate_limit_rule(
        self,
        rule: RateLimitRule,
        client_key: str,
        complexity_score: ComplexityScore,
        operation_type: QueryType
    ) -> Dict[str, Any]:
        """Check a specific rate limiting rule"""

        now = datetime.utcnow()
        window_start = now - timedelta(seconds=rule.window_seconds)

        # Get or create client state
        if client_key not in self.rate_limit_states:
            self.rate_limit_states[client_key] = RateLimitState(
                client_id=client_key,
                window_start=window_start,
                request_count=0,
                complexity_used=0,
                blocked_until=None,
                violations=[],
                last_request=now
            )

        state = self.rate_limit_states[client_key]

        # Reset window if needed
        if state.window_start < window_start:
            state.window_start = window_start
            state.request_count = 0
            state.complexity_used = 0

        # Check rule conditions
        if not self._rule_conditions_met(rule, complexity_score, operation_type):
            return {'limited': False}

        # Check specific rule type
        if rule.limit_type == RateLimitType.QUERY_COUNT:
            if state.request_count >= rule.max_requests + rule.burst_allowance:
                return {
                    'limited': True,
                    'reason': f'query_count_exceeded_{rule.name}',
                    'retry_after_seconds': rule.window_seconds,
                    'rule': rule.name
                }

        elif rule.limit_type == RateLimitType.COMPLEXITY_BUDGET:
            projected_complexity = state.complexity_used + complexity_score.total_complexity
            if projected_complexity > rule.complexity_threshold + rule.burst_allowance:
                return {
                    'limited': True,
                    'reason': f'complexity_budget_exceeded_{rule.name}',
                    'retry_after_seconds': rule.window_seconds,
                    'rule': rule.name
                }

        elif rule.limit_type == RateLimitType.DEPTH_LIMIT:
            min_depth = rule.conditions.get('min_depth', 0)
            if complexity_score.analysis_details.get('max_depth', 0) >= min_depth:
                if state.request_count >= rule.max_requests + rule.burst_allowance:
                    return {
                        'limited': True,
                        'reason': f'depth_limit_exceeded_{rule.name}',
                        'retry_after_seconds': rule.penalty_duration_seconds,
                        'rule': rule.name
                    }

        return {'limited': False}

    def _rule_conditions_met(
        self,
        rule: RateLimitRule,
        complexity_score: ComplexityScore,
        operation_type: QueryType
    ) -> bool:
        """Check if rule conditions are met for this query"""

        conditions = rule.conditions

        # Check minimum complexity
        if 'min_complexity' in conditions:
            if complexity_score.total_complexity < conditions['min_complexity']:
                return False

        # Check minimum depth
        if 'min_depth' in conditions:
            if complexity_score.analysis_details.get('max_depth', 0) < conditions['min_depth']:
                return False

        # Check operation type
        if 'operation_types' in conditions:
            if operation_type.value not in conditions['operation_types']:
                return False

        return True

    async def _update_rate_limit_state(
        self,
        client_key: str,
        rule: RateLimitRule,
        limit_result: Dict[str, Any],
        complexity_score: ComplexityScore
    ):
        """Update rate limit state after a rule violation"""

        state = self.rate_limit_states[client_key]

        # Add violation record
        violation = {
            'timestamp': datetime.utcnow(),
            'rule': rule.name,
            'complexity': complexity_score.total_complexity,
            'reason': limit_result['reason']
        }
        state.violations.append(violation)

        # Keep only recent violations (last 24 hours)
        cutoff_time = datetime.utcnow() - timedelta(hours=24)
        state.violations = [v for v in state.violations if v['timestamp'] > cutoff_time]

        # Apply penalty if multiple violations
        if len(state.violations) >= 3:  # 3 violations trigger penalty
            state.blocked_until = datetime.utcnow() + timedelta(seconds=rule.penalty_duration_seconds)
            self.logger.warning(f"Client {client_key} blocked until {state.blocked_until} due to multiple violations")

        # Store state in Redis if available
        if self.redis_client:
            try:
                redis_key = f"graphql_rate_limit:{client_key}"
                state_data = {
                    'window_start': state.window_start.isoformat(),
                    'request_count': state.request_count,
                    'complexity_used': state.complexity_used,
                    'blocked_until': state.blocked_until.isoformat() if state.blocked_until else None,
                    'violations': state.violations[-10:],  # Keep last 10 violations
                    'last_request': state.last_request.isoformat()
                }
                await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: self.redis_client.setex(
                        redis_key,
                        rule.window_seconds * 2,
                        json.dumps(state_data, default=str)
                    )
                )
            except Exception as e:
                self.logger.error(f"Failed to update Redis state: {e}")

    async def _update_successful_request_state(
        self,
        client_key: str,
        complexity_score: ComplexityScore
    ):
        """Update state for successful requests"""

        state = self.rate_limit_states.get(client_key)
        if state:
            state.request_count += 1
            state.complexity_used += complexity_score.total_complexity
            state.last_request = datetime.utcnow()

    def _detect_malicious_patterns(self, document: DocumentNode, complexity_score: ComplexityScore):
        """Detect potentially malicious query patterns"""

        query_string = str(document)

        # Check for known malicious patterns
        malicious_indicators = [
            # Excessive nesting
            '{{{{{{',
            # Introspection abuse
            '__schema' * 3,
            # Large list queries
            'first:9999',
            'limit:9999',
            # Circular references
            'user{posts{user{posts{user',
        ]

        for pattern in malicious_indicators:
            if pattern in query_string.replace(' ', '').replace('\n', ''):
                self.malicious_patterns.add(pattern)
                complexity_score.level = ComplexityLevel.BLOCKED
                self.logger.warning(f"Malicious pattern detected: {pattern}")

    def _generate_query_id(self, document: DocumentNode, variables: Optional[Dict]) -> str:
        """Generate unique query ID for tracking"""
        query_string = str(document)
        variables_string = json.dumps(variables or {}, sort_keys=True)
        combined = f"{query_string}:{variables_string}"
        return hashlib.sha256(combined.encode()).hexdigest()[:16]

    def _extract_operation(self, document: DocumentNode, operation_name: Optional[str]) -> OperationDefinitionNode:
        """Extract the operation to analyze"""
        operations = [node for node in document.definitions if isinstance(node, OperationDefinitionNode)]

        if not operations:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="No operations found in query"
            )

        if operation_name:
            named_operations = [op for op in operations if op.name and op.name.value == operation_name]
            if not named_operations:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=f"Operation '{operation_name}' not found"
                )
            return named_operations[0]

        return operations[0]

    def _get_operation_type(self, operation: OperationDefinitionNode) -> QueryType:
        """Get operation type"""
        if operation.operation == OperationType.QUERY:
            return QueryType.QUERY
        elif operation.operation == OperationType.MUTATION:
            return QueryType.MUTATION
        elif operation.operation == OperationType.SUBSCRIPTION:
            return QueryType.SUBSCRIPTION
        else:
            return QueryType.QUERY

    def _count_fields(self, document: DocumentNode) -> int:
        """Count total fields in the query"""
        field_count = 0

        def count_fields_visitor(node, *_):
            nonlocal field_count
            if isinstance(node, FieldNode):
                field_count += 1

        visit(document, {'enter': count_fields_visitor})
        return field_count

    def _calculate_max_depth(self, document: DocumentNode) -> int:
        """Calculate maximum query depth"""
        max_depth = 0

        def depth_visitor(node, key, parent, path, ancestors):
            nonlocal max_depth
            if isinstance(node, FieldNode):
                # Count depth based on ancestor FieldNodes
                field_depth = len([a for a in ancestors if isinstance(a, FieldNode)])
                max_depth = max(max_depth, field_depth)

        visit(document, {'enter': depth_visitor})
        return max_depth

    def _has_fragments(self, document: DocumentNode) -> bool:
        """Check if query uses fragments"""
        for definition in document.definitions:
            if isinstance(definition, FragmentDefinitionNode):
                return True

        def fragment_visitor(node, *_):
            if isinstance(node, InlineFragmentNode):
                return False  # Stop visiting, we found a fragment

        result = visit(document, {'enter': fragment_visitor})
        return result != document  # If visit returned early, fragments were found

    async def get_query_metrics(
        self,
        time_range_minutes: int = 60,
        client_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """Get comprehensive query metrics and analytics"""

        end_time = datetime.utcnow()
        start_time = end_time - timedelta(minutes=time_range_minutes)

        # Filter metrics by time range and client
        filtered_metrics = []
        for metrics in self.query_metrics.values():
            if start_time <= metrics.timestamp <= end_time:
                if not client_id or metrics.client_id == client_id:
                    filtered_metrics.append(metrics)

        if not filtered_metrics:
            return {
                'time_range': {'start': start_time.isoformat(), 'end': end_time.isoformat()},
                'total_queries': 0,
                'message': 'No queries found in the specified time range'
            }

        # Calculate analytics
        total_queries = len(filtered_metrics)
        blocked_queries = len([m for m in filtered_metrics if m.rate_limit_applied])
        avg_complexity = sum(m.complexity_score.total_complexity for m in filtered_metrics) / total_queries
        avg_execution_time = sum(m.execution_time_ms or 0 for m in filtered_metrics) / total_queries

        complexity_distribution = {
            'low': len([m for m in filtered_metrics if m.complexity_score.level == ComplexityLevel.LOW]),
            'medium': len([m for m in filtered_metrics if m.complexity_score.level == ComplexityLevel.MEDIUM]),
            'high': len([m for m in filtered_metrics if m.complexity_score.level == ComplexityLevel.HIGH]),
            'critical': len([m for m in filtered_metrics if m.complexity_score.level == ComplexityLevel.CRITICAL]),
            'blocked': len([m for m in filtered_metrics if m.complexity_score.level == ComplexityLevel.BLOCKED])
        }

        operation_types = {
            'query': len([m for m in filtered_metrics if m.operation_type == QueryType.QUERY]),
            'mutation': len([m for m in filtered_metrics if m.operation_type == QueryType.MUTATION]),
            'subscription': len([m for m in filtered_metrics if m.operation_type == QueryType.SUBSCRIPTION])
        }

        return {
            'time_range': {
                'start': start_time.isoformat(),
                'end': end_time.isoformat(),
                'duration_minutes': time_range_minutes
            },
            'query_statistics': {
                'total_queries': total_queries,
                'blocked_queries': blocked_queries,
                'success_rate': ((total_queries - blocked_queries) / total_queries * 100) if total_queries else 0,
                'average_complexity': round(avg_complexity, 2),
                'average_execution_time_ms': round(avg_execution_time, 2)
            },
            'complexity_distribution': complexity_distribution,
            'operation_types': operation_types,
            'top_clients': self._get_top_clients(filtered_metrics),
            'performance_trends': self._calculate_performance_trends(filtered_metrics),
            'security_metrics': {
                'malicious_patterns_detected': len(self.malicious_patterns),
                'rate_limit_violations': len([m for m in filtered_metrics if m.blocked_reason]),
                'introspection_queries': len([m for m in filtered_metrics if '__schema' in str(m.operation_name or '')])
            }
        }

    def _get_top_clients(self, metrics: List[QueryMetrics]) -> List[Dict[str, Any]]:
        """Get top clients by query volume and complexity"""
        client_stats = defaultdict(lambda: {'queries': 0, 'total_complexity': 0, 'blocked': 0})

        for metric in metrics:
            client_id = metric.client_id
            client_stats[client_id]['queries'] += 1
            client_stats[client_id]['total_complexity'] += metric.complexity_score.total_complexity
            if metric.rate_limit_applied:
                client_stats[client_id]['blocked'] += 1

        # Sort by total complexity and return top 10
        top_clients = []
        for client_id, stats in sorted(
            client_stats.items(),
            key=lambda x: x[1]['total_complexity'],
            reverse=True
        )[:10]:
            stats['client_id'] = client_id
            stats['avg_complexity'] = stats['total_complexity'] / stats['queries']
            top_clients.append(stats)

        return top_clients

    def _calculate_performance_trends(self, metrics: List[QueryMetrics]) -> Dict[str, Any]:
        """Calculate performance trends over time"""
        if len(metrics) < 10:
            return {'message': 'Insufficient data for trend analysis'}

        # Sort by timestamp
        sorted_metrics = sorted(metrics, key=lambda m: m.timestamp)

        # Split into first and second half
        mid_point = len(sorted_metrics) // 2
        first_half = sorted_metrics[:mid_point]
        second_half = sorted_metrics[mid_point:]

        first_avg_complexity = sum(m.complexity_score.total_complexity for m in first_half) / len(first_half)
        second_avg_complexity = sum(m.complexity_score.total_complexity for m in second_half) / len(second_half)

        first_avg_time = sum(m.execution_time_ms or 0 for m in first_half) / len(first_half)
        second_avg_time = sum(m.execution_time_ms or 0 for m in second_half) / len(second_half)

        return {
            'complexity_trend': {
                'direction': 'increasing' if second_avg_complexity > first_avg_complexity else 'decreasing',
                'change_percent': ((second_avg_complexity - first_avg_complexity) / first_avg_complexity * 100) if first_avg_complexity else 0
            },
            'execution_time_trend': {
                'direction': 'increasing' if second_avg_time > first_avg_time else 'decreasing',
                'change_percent': ((second_avg_time - first_avg_time) / first_avg_time * 100) if first_avg_time else 0
            }
        }


class ComplexityAnalysisVisitor(Visitor):
    """Visitor for calculating GraphQL query complexity"""

    def __init__(
        self,
        schema: GraphQLSchema,
        field_complexity_map: Dict[str, int],
        resolver_complexity_map: Dict[str, int],
        default_complexity: int,
        list_multiplier: int,
        variables: Dict[str, Any]
    ):
        self.schema = schema
        self.field_complexity_map = field_complexity_map
        self.resolver_complexity_map = resolver_complexity_map
        self.default_complexity = default_complexity
        self.list_multiplier = list_multiplier
        self.variables = variables

        # Tracking variables
        self.field_complexity = 0
        self.list_complexity = 0
        self.resolver_complexity = 0
        self.max_depth = 0
        self.current_depth = 0
        self.field_count = 0
        self.list_field_count = 0
        self.resolver_count = 0
        self.introspection_field_count = 0
        self.expensive_fields = []

        # Type information for context
        self.type_info = TypeInfo(schema)

    def enter_field(self, node: FieldNode, *_):
        """Process field entry"""
        self.current_depth += 1
        self.max_depth = max(self.max_depth, self.current_depth)
        self.field_count += 1

        field_name = node.name.value
        field_complexity = self.field_complexity_map.get(field_name, self.default_complexity)

        # Check for introspection fields
        if field_name.startswith('__'):
            self.introspection_field_count += 1
            field_complexity = max(field_complexity, 10)  # Minimum complexity for introspection

        # Check if field returns a list
        current_type = self.type_info.get_type()
        if current_type and is_list_type(get_named_type(current_type)):
            self.list_field_count += 1

            # Calculate list complexity based on arguments
            list_multiplier = self._calculate_list_multiplier(node)
            list_complexity_addition = field_complexity * list_multiplier
            self.list_complexity += list_complexity_addition

            if list_multiplier > 10:  # Large list query
                self.expensive_fields.append({
                    'field': field_name,
                    'complexity': list_complexity_addition,
                    'list_multiplier': list_multiplier
                })

        # Add base field complexity
        self.field_complexity += field_complexity

        # Track expensive fields
        if field_complexity > 20:
            self.expensive_fields.append({
                'field': field_name,
                'complexity': field_complexity,
                'type': 'expensive_field'
            })

    def leave_field(self, node: FieldNode, *_):
        """Process field exit"""
        self.current_depth -= 1

    def _calculate_list_multiplier(self, field_node: FieldNode) -> int:
        """Calculate multiplier for list fields based on arguments"""
        multiplier = self.list_multiplier

        if field_node.arguments:
            for arg in field_node.arguments:
                arg_name = arg.name.value
                arg_value = self._get_argument_value(arg)

                # Common pagination arguments
                if arg_name in ['first', 'last', 'limit', 'count']:
                    if isinstance(arg_value, int):
                        multiplier = min(arg_value, 100)  # Cap at 100

                # Skip/offset arguments add to complexity
                elif arg_name in ['skip', 'offset']:
                    if isinstance(arg_value, int):
                        multiplier += arg_value // 10  # Add complexity for skipping

        return max(multiplier, 1)

    def _get_argument_value(self, arg_node) -> Any:
        """Get the value of an argument node"""
        from graphql.language import ast

        if isinstance(arg_node.value, ast.IntValueNode):
            return int(arg_node.value.value)
        elif isinstance(arg_node.value, ast.StringValueNode):
            return arg_node.value.value
        elif isinstance(arg_node.value, ast.BooleanValueNode):
            return arg_node.value.value
        elif isinstance(arg_node.value, ast.VariableNode):
            var_name = arg_node.value.name.value
            return self.variables.get(var_name)

        return None


# Global instance
_query_complexity_analyzer: Optional[GraphQLQueryComplexityAnalyzer] = None


async def get_query_complexity_analyzer(schema: GraphQLSchema) -> GraphQLQueryComplexityAnalyzer:
    """Get or create the query complexity analyzer instance"""
    global _query_complexity_analyzer

    if _query_complexity_analyzer is None:
        _query_complexity_analyzer = GraphQLQueryComplexityAnalyzer(schema)
        await _query_complexity_analyzer.initialize()

    return _query_complexity_analyzer


# Custom validation rule for complexity analysis
class QueryComplexityRule(ValidationRule):
    """Custom validation rule that integrates with complexity analysis"""

    def __init__(self, max_complexity: int = 1000, max_depth: int = 15):
        self.max_complexity = max_complexity
        self.max_depth = max_depth

    def __call__(self, context: ValidationContext):
        # This would integrate with the complexity analyzer
        # Implementation would check complexity during validation phase
        pass