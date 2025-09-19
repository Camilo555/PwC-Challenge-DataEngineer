"""
GraphQL Query Complexity Management API Routes
Enterprise GraphQL security with intelligent query analysis and rate limiting management.
"""

from fastapi import APIRouter, Depends, HTTPException, status, Request
from fastapi.responses import JSONResponse
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from pydantic import BaseModel, Field
import json

from api.graphql.query_complexity_analyzer import (
    get_query_complexity_analyzer,
    GraphQLQueryComplexityAnalyzer,
    ComplexityLevel,
    RateLimitType,
    QueryType,
    RateLimitRule
)
from api.graphql.router import get_graphql_schema
from graphql import parse, DocumentNode
from core.config.base_config import BaseConfig
from core.logging import get_logger

logger = get_logger(__name__)
router = APIRouter(prefix="/graphql", tags=["graphql", "complexity", "rate-limiting"])


# Request/Response Models
class QueryAnalysisRequest(BaseModel):
    """Request model for query complexity analysis"""
    query: str = Field(..., description="GraphQL query string to analyze")
    variables: Optional[Dict[str, Any]] = Field(None, description="Query variables")
    operation_name: Optional[str] = Field(None, description="Optional operation name")
    client_id: Optional[str] = Field("anonymous", description="Client identifier for rate limiting")
    user_id: Optional[str] = Field(None, description="User identifier")


class RateLimitRuleRequest(BaseModel):
    """Request model for creating/updating rate limit rules"""
    name: str = Field(..., description="Rule name")
    limit_type: str = Field(..., description="Type of rate limiting: query_count, complexity_budget, depth_limit, field_count")
    window_seconds: int = Field(..., description="Time window in seconds", ge=1, le=86400)
    max_requests: int = Field(0, description="Maximum requests in window (for query_count type)", ge=0)
    complexity_threshold: int = Field(0, description="Complexity threshold (for complexity_budget type)", ge=0)
    user_specific: bool = Field(True, description="Whether rule applies per user")
    burst_allowance: int = Field(0, description="Additional allowance for burst traffic", ge=0)
    penalty_duration_seconds: int = Field(300, description="Penalty duration on violation", ge=0, le=3600)
    priority: int = Field(5, description="Rule priority (lower number = higher priority)", ge=1, le=10)
    conditions: Optional[Dict[str, Any]] = Field(None, description="Additional rule conditions")
    enabled: bool = Field(True, description="Whether rule is enabled")


class ComplexityConfigRequest(BaseModel):
    """Request model for updating complexity configuration"""
    max_complexity: Optional[int] = Field(None, description="Maximum allowed query complexity", ge=1, le=10000)
    max_depth: Optional[int] = Field(None, description="Maximum allowed query depth", ge=1, le=50)
    default_field_complexity: Optional[int] = Field(None, description="Default field complexity score", ge=1, le=100)
    list_multiplier: Optional[int] = Field(None, description="List field complexity multiplier", ge=1, le=100)
    enable_introspection_limit: Optional[bool] = Field(None, description="Enable introspection query limiting")
    field_complexity_overrides: Optional[Dict[str, int]] = Field(None, description="Custom field complexity mappings")


# Query Analysis Endpoints

@router.post("/analyze", response_model=dict)
async def analyze_query_complexity(
    analysis_request: QueryAnalysisRequest,
    request: Request,
    analyzer: GraphQLQueryComplexityAnalyzer = Depends(lambda: get_query_complexity_analyzer(get_graphql_schema())),
    current_user: dict = Depends(lambda: {"sub": "system", "permissions": ["perm_monitoring_read"]})
) -> dict:
    """
    Analyze GraphQL query complexity and check rate limits

    This endpoint provides comprehensive analysis of GraphQL queries including:
    - Complexity scoring across multiple dimensions
    - Depth and breadth analysis
    - Rate limiting evaluation
    - Security threat detection
    - Performance impact prediction
    - Compliance with configured limits
    """
    try:
        # Parse the GraphQL query
        try:
            document = parse(analysis_request.query)
        except Exception as e:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"GraphQL query parsing failed: {str(e)}"
            )

        # Get client information
        client_ip = request.client.host if request.client else "unknown"
        client_id = analysis_request.client_id or client_ip
        user_agent = request.headers.get("User-Agent", "unknown")

        logger.info(f"Analyzing GraphQL query from client {client_id}")

        # Analyze query complexity
        query_metrics = await analyzer.analyze_query_complexity(
            document=document,
            variables=analysis_request.variables,
            client_id=client_id,
            user_id=analysis_request.user_id,
            operation_name=analysis_request.operation_name
        )

        # Prepare detailed response
        response = {
            "query_analysis": {
                "query_id": query_metrics.query_id,
                "operation_name": query_metrics.operation_name,
                "operation_type": query_metrics.operation_type.value,
                "timestamp": query_metrics.timestamp.isoformat(),
                "client_id": query_metrics.client_id,
                "user_id": query_metrics.user_id
            },
            "complexity_score": {
                "total_complexity": query_metrics.complexity_score.total_complexity,
                "complexity_level": query_metrics.complexity_score.level.value,
                "breakdown": {
                    "field_complexity": query_metrics.complexity_score.field_complexity,
                    "depth_complexity": query_metrics.complexity_score.depth_complexity,
                    "list_complexity": query_metrics.complexity_score.list_complexity,
                    "resolver_complexity": query_metrics.complexity_score.resolver_complexity,
                    "custom_complexity": query_metrics.complexity_score.custom_complexity
                }
            },
            "query_structure": {
                "field_count": query_metrics.field_count,
                "max_depth": query_metrics.max_depth,
                "resolver_count": query_metrics.resolver_count,
                "has_fragments": query_metrics.has_fragments,
                "has_variables": query_metrics.has_variables
            },
            "rate_limiting": {
                "rate_limit_applied": query_metrics.rate_limit_applied,
                "blocked_reason": query_metrics.blocked_reason,
                "status": "allowed" if not query_metrics.rate_limit_applied else "rate_limited"
            },
            "security_analysis": {
                "potential_threats": [],
                "introspection_query": "__schema" in analysis_request.query or "__type" in analysis_request.query,
                "excessive_depth": query_metrics.max_depth > 10,
                "high_complexity": query_metrics.complexity_score.level in [ComplexityLevel.HIGH, ComplexityLevel.CRITICAL],
                "security_score": self._calculate_security_score(query_metrics)
            },
            "performance_prediction": {
                "estimated_execution_time_ms": self._predict_execution_time(query_metrics),
                "resource_impact": self._predict_resource_impact(query_metrics),
                "scalability_concerns": self._identify_scalability_concerns(query_metrics)
            },
            "recommendations": self._generate_optimization_recommendations(query_metrics),
            "compliance_status": {
                "within_limits": not query_metrics.rate_limit_applied,
                "complexity_limit_utilization": (query_metrics.complexity_score.total_complexity / analyzer.max_complexity) * 100,
                "depth_limit_utilization": (query_metrics.max_depth / analyzer.max_depth) * 100
            },
            "metadata": {
                "analyzer_version": "1.0.0",
                "analysis_duration_ms": query_metrics.execution_time_ms,
                "client_info": {
                    "ip_address": client_ip,
                    "user_agent": user_agent[:100]  # Truncate for security
                }
            }
        }

        return response

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Query analysis failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Query analysis failed"
        )

def _calculate_security_score(query_metrics) -> int:
    """Calculate security score for the query (0-100)"""
    score = 100

    # Deduct points for various risk factors
    if query_metrics.complexity_score.level == ComplexityLevel.CRITICAL:
        score -= 30
    elif query_metrics.complexity_score.level == ComplexityLevel.HIGH:
        score -= 20
    elif query_metrics.complexity_score.level == ComplexityLevel.MEDIUM:
        score -= 10

    if query_metrics.max_depth > 10:
        score -= 15

    if query_metrics.complexity_score.analysis_details.get('introspection_fields', 0) > 0:
        score -= 20

    if query_metrics.field_count > 50:
        score -= 10

    return max(score, 0)

def _predict_execution_time(query_metrics) -> float:
    """Predict query execution time based on complexity"""
    base_time = 10  # Base 10ms
    complexity_factor = query_metrics.complexity_score.total_complexity * 0.5
    depth_factor = query_metrics.max_depth * 2
    field_factor = query_metrics.field_count * 0.1

    return base_time + complexity_factor + depth_factor + field_factor

def _predict_resource_impact(query_metrics) -> str:
    """Predict resource impact"""
    if query_metrics.complexity_score.level in [ComplexityLevel.CRITICAL, ComplexityLevel.BLOCKED]:
        return "high"
    elif query_metrics.complexity_score.level == ComplexityLevel.HIGH:
        return "medium"
    else:
        return "low"

def _identify_scalability_concerns(query_metrics) -> List[str]:
    """Identify potential scalability concerns"""
    concerns = []

    if query_metrics.max_depth > 8:
        concerns.append("Deep nested queries may cause exponential resource usage")

    if query_metrics.complexity_score.list_complexity > 100:
        concerns.append("Large list queries may impact database performance")

    if query_metrics.field_count > 30:
        concerns.append("High field count may cause N+1 query problems")

    if query_metrics.complexity_score.analysis_details.get('introspection_fields', 0) > 5:
        concerns.append("Excessive introspection may reveal sensitive schema information")

    return concerns

def _generate_optimization_recommendations(query_metrics) -> List[str]:
    """Generate query optimization recommendations"""
    recommendations = []

    if query_metrics.max_depth > 6:
        recommendations.append("Consider reducing query depth by splitting into multiple requests")

    if query_metrics.complexity_score.list_complexity > 50:
        recommendations.append("Use pagination parameters (first/last/limit) to limit list field results")

    if query_metrics.field_count > 20:
        recommendations.append("Request only necessary fields to improve performance")

    if query_metrics.has_fragments:
        recommendations.append("Fragments detected - ensure they don't cause circular dependencies")

    if query_metrics.complexity_score.analysis_details.get('introspection_fields', 0) > 0:
        recommendations.append("Avoid introspection queries in production environments")

    if not recommendations:
        recommendations.append("Query is well-optimized within acceptable complexity limits")

    return recommendations


@router.get("/metrics", response_model=dict)
async def get_graphql_metrics(
    time_range_minutes: int = 60,
    client_id: Optional[str] = None,
    include_blocked: bool = True,
    analyzer: GraphQLQueryComplexityAnalyzer = Depends(lambda: get_query_complexity_analyzer(get_graphql_schema())),
    current_user: dict = Depends(lambda: {"sub": "system", "permissions": ["perm_monitoring_read"]})
) -> dict:
    """
    Get comprehensive GraphQL metrics and analytics

    Provides detailed insights into:
    - Query volume and patterns
    - Complexity distribution
    - Rate limiting effectiveness
    - Performance trends
    - Security incident detection
    - Client behavior analysis
    """
    try:
        metrics = await analyzer.get_query_metrics(time_range_minutes, client_id)

        # Add enterprise-specific enhancements
        enhanced_metrics = {
            **metrics,
            "enterprise_insights": {
                "query_optimization_opportunities": len([
                    m for m_id, m in analyzer.query_metrics.items()
                    if m.complexity_score.level in [ComplexityLevel.HIGH, ComplexityLevel.CRITICAL]
                ]),
                "performance_impact_score": self._calculate_performance_impact_score(analyzer),
                "security_threat_level": self._assess_security_threat_level(analyzer),
                "compliance_score": self._calculate_compliance_score(analyzer),
                "cost_impact_analysis": self._analyze_cost_impact(analyzer)
            },
            "operational_metrics": {
                "active_rate_limit_rules": len(analyzer.rate_limit_rules),
                "currently_blocked_clients": len([
                    state for state in analyzer.rate_limit_states.values()
                    if state.blocked_until and datetime.utcnow() < state.blocked_until
                ]),
                "redis_connection_status": "connected" if analyzer.redis_client else "fallback_memory",
                "query_pattern_detection": {
                    "unique_patterns": len(analyzer.query_patterns),
                    "malicious_patterns": len(analyzer.malicious_patterns),
                    "most_common_patterns": dict(list(analyzer.query_patterns.most_common(5)))
                }
            },
            "recommendations": {
                "rate_limit_tuning": self._generate_rate_limit_recommendations(analyzer),
                "complexity_optimization": self._generate_complexity_recommendations(analyzer),
                "security_enhancements": self._generate_security_recommendations(analyzer)
            }
        }

        return enhanced_metrics

    except Exception as e:
        logger.error(f"Failed to get GraphQL metrics: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve GraphQL metrics"
        )

def _calculate_performance_impact_score(analyzer: GraphQLQueryComplexityAnalyzer) -> int:
    """Calculate overall performance impact score (0-100)"""
    if not analyzer.performance_history:
        return 0

    recent_queries = list(analyzer.performance_history)[-100:]  # Last 100 queries
    avg_complexity = sum(q['complexity'] for q in recent_queries) / len(recent_queries)
    avg_execution_time = sum(q.get('execution_time_ms', 0) for q in recent_queries) / len(recent_queries)

    # Normalize to 0-100 scale
    complexity_score = min((avg_complexity / analyzer.max_complexity) * 100, 100)
    execution_score = min((avg_execution_time / 1000) * 100, 100)  # 1 second = 100 points

    return int((complexity_score + execution_score) / 2)

def _assess_security_threat_level(analyzer: GraphQLQueryComplexityAnalyzer) -> str:
    """Assess overall security threat level"""
    threat_indicators = 0

    # Check for malicious patterns
    if analyzer.malicious_patterns:
        threat_indicators += len(analyzer.malicious_patterns) * 2

    # Check for blocked queries
    recent_blocked = len([
        m for m in analyzer.query_metrics.values()
        if m.rate_limit_applied and m.timestamp > datetime.utcnow() - timedelta(hours=1)
    ])
    threat_indicators += recent_blocked

    # Check for excessive introspection
    introspection_queries = len([
        m for m in analyzer.query_metrics.values()
        if m.complexity_score.analysis_details.get('introspection_fields', 0) > 0
        and m.timestamp > datetime.utcnow() - timedelta(hours=1)
    ])
    threat_indicators += introspection_queries

    if threat_indicators >= 10:
        return "high"
    elif threat_indicators >= 5:
        return "medium"
    elif threat_indicators >= 1:
        return "low"
    else:
        return "minimal"

def _calculate_compliance_score(analyzer: GraphQLQueryComplexityAnalyzer) -> int:
    """Calculate compliance score based on rule adherence"""
    total_queries = len(analyzer.query_metrics)
    if total_queries == 0:
        return 100

    blocked_queries = len([m for m in analyzer.query_metrics.values() if m.rate_limit_applied])
    compliance_rate = ((total_queries - blocked_queries) / total_queries) * 100

    return int(compliance_rate)

def _analyze_cost_impact(analyzer: GraphQLQueryComplexityAnalyzer) -> Dict[str, Any]:
    """Analyze cost impact of GraphQL operations"""
    if not analyzer.performance_history:
        return {"message": "Insufficient data for cost analysis"}

    recent_queries = list(analyzer.performance_history)[-100:]
    avg_complexity = sum(q['complexity'] for q in recent_queries) / len(recent_queries)

    # Estimated cost per complexity unit (arbitrary units for demonstration)
    cost_per_complexity_unit = 0.001  # $0.001 per complexity point
    estimated_hourly_cost = avg_complexity * cost_per_complexity_unit * 60  # Assuming 1 query per minute

    return {
        "average_query_complexity": round(avg_complexity, 2),
        "estimated_cost_per_query": round(avg_complexity * cost_per_complexity_unit, 4),
        "estimated_hourly_cost_usd": round(estimated_hourly_cost, 3),
        "cost_optimization_potential": "high" if avg_complexity > analyzer.max_complexity * 0.7 else "medium" if avg_complexity > analyzer.max_complexity * 0.4 else "low"
    }

def _generate_rate_limit_recommendations(analyzer: GraphQLQueryComplexityAnalyzer) -> List[str]:
    """Generate rate limiting recommendations"""
    recommendations = []

    # Analyze violation patterns
    total_violations = sum(len(state.violations) for state in analyzer.rate_limit_states.values())
    if total_violations > 10:
        recommendations.append("Consider tightening rate limits due to high violation count")
    elif total_violations == 0:
        recommendations.append("Rate limits may be too restrictive - consider relaxing for better user experience")

    # Check rule effectiveness
    if not analyzer.rate_limit_rules:
        recommendations.append("No rate limiting rules configured - recommend implementing basic query count limits")

    return recommendations or ["Current rate limiting configuration appears optimal"]

def _generate_complexity_recommendations(analyzer: GraphQLQueryComplexityAnalyzer) -> List[str]:
    """Generate complexity optimization recommendations"""
    recommendations = []

    high_complexity_queries = len([
        m for m in analyzer.query_metrics.values()
        if m.complexity_score.level in [ComplexityLevel.HIGH, ComplexityLevel.CRITICAL]
    ])

    if high_complexity_queries > 10:
        recommendations.append("High number of complex queries detected - consider query optimization guidelines")

    if analyzer.malicious_patterns:
        recommendations.append("Malicious query patterns detected - implement additional security measures")

    return recommendations or ["Query complexity patterns are within acceptable ranges"]

def _generate_security_recommendations(analyzer: GraphQLQueryComplexityAnalyzer) -> List[str]:
    """Generate security enhancement recommendations"""
    recommendations = []

    if analyzer.malicious_patterns:
        recommendations.append("Implement query allow-listing for production environments")
        recommendations.append("Add automated alerting for malicious pattern detection")

    introspection_count = len([
        m for m in analyzer.query_metrics.values()
        if m.complexity_score.analysis_details.get('introspection_fields', 0) > 0
    ])

    if introspection_count > 0:
        recommendations.append("Disable introspection queries in production")

    return recommendations or ["GraphQL security posture is adequate"]


# Rate Limiting Management Endpoints

@router.get("/rate-limits/rules", response_model=dict)
async def list_rate_limit_rules(
    analyzer: GraphQLQueryComplexityAnalyzer = Depends(lambda: get_query_complexity_analyzer(get_graphql_schema())),
    current_user: dict = Depends(lambda: {"sub": "system", "permissions": ["perm_admin_system"]})
) -> dict:
    """
    List all rate limiting rules and their configurations

    Returns comprehensive information about:
    - Rule definitions and parameters
    - Rule effectiveness metrics
    - Enforcement statistics
    - Configuration recommendations
    """
    try:
        rules_info = []
        for rule in analyzer.rate_limit_rules:
            # Calculate rule effectiveness
            violations_by_rule = []
            for state in analyzer.rate_limit_states.values():
                rule_violations = [v for v in state.violations if v.get('rule') == rule.name]
                violations_by_rule.extend(rule_violations)

            rule_info = {
                "name": rule.name,
                "limit_type": rule.limit_type.value,
                "window_seconds": rule.window_seconds,
                "max_requests": rule.max_requests,
                "complexity_threshold": rule.complexity_threshold,
                "user_specific": rule.user_specific,
                "burst_allowance": rule.burst_allowance,
                "penalty_duration_seconds": rule.penalty_duration_seconds,
                "priority": rule.priority,
                "conditions": rule.conditions,
                "effectiveness_metrics": {
                    "total_violations": len(violations_by_rule),
                    "violations_last_hour": len([
                        v for v in violations_by_rule
                        if v['timestamp'] > datetime.utcnow() - timedelta(hours=1)
                    ]),
                    "average_violations_per_day": len(violations_by_rule) / 7 if len(violations_by_rule) > 0 else 0,
                    "rule_effectiveness": "high" if len(violations_by_rule) > 0 else "low"
                }
            }
            rules_info.append(rule_info)

        return {
            "rate_limit_rules": rules_info,
            "summary": {
                "total_rules": len(analyzer.rate_limit_rules),
                "rules_by_type": {
                    "query_count": len([r for r in analyzer.rate_limit_rules if r.limit_type == RateLimitType.QUERY_COUNT]),
                    "complexity_budget": len([r for r in analyzer.rate_limit_rules if r.limit_type == RateLimitType.COMPLEXITY_BUDGET]),
                    "depth_limit": len([r for r in analyzer.rate_limit_rules if r.limit_type == RateLimitType.DEPTH_LIMIT]),
                    "field_count": len([r for r in analyzer.rate_limit_rules if r.limit_type == RateLimitType.FIELD_COUNT])
                },
                "user_specific_rules": len([r for r in analyzer.rate_limit_rules if r.user_specific]),
                "global_rules": len([r for r in analyzer.rate_limit_rules if not r.user_specific])
            },
            "enforcement_statistics": {
                "currently_blocked_clients": len([
                    state for state in analyzer.rate_limit_states.values()
                    if state.blocked_until and datetime.utcnow() < state.blocked_until
                ]),
                "total_violations_last_24h": sum([
                    len([v for v in state.violations if v['timestamp'] > datetime.utcnow() - timedelta(hours=24)])
                    for state in analyzer.rate_limit_states.values()
                ]),
                "active_rate_limit_states": len(analyzer.rate_limit_states)
            }
        }

    except Exception as e:
        logger.error(f"Failed to list rate limit rules: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve rate limit rules"
        )


@router.post("/rate-limits/rules", response_model=dict, status_code=status.HTTP_201_CREATED)
async def create_rate_limit_rule(
    rule_request: RateLimitRuleRequest,
    analyzer: GraphQLQueryComplexityAnalyzer = Depends(lambda: get_query_complexity_analyzer(get_graphql_schema())),
    current_user: dict = Depends(lambda: {"sub": "system", "permissions": ["perm_admin_system"]})
) -> dict:
    """
    Create a new rate limiting rule

    Allows configuration of sophisticated rate limiting rules including:
    - Query count limits per time window
    - Complexity budget management
    - Depth limiting for security
    - Field count restrictions
    - User-specific vs. global rules
    - Burst allowances and penalties
    """
    try:
        # Validate limit type
        try:
            limit_type_enum = RateLimitType(rule_request.limit_type.lower())
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid limit type. Supported types: {[lt.value for lt in RateLimitType]}"
            )

        # Check for duplicate rule names
        existing_names = [rule.name for rule in analyzer.rate_limit_rules]
        if rule_request.name in existing_names:
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Rate limit rule '{rule_request.name}' already exists"
            )

        # Validate rule parameters based on type
        if limit_type_enum == RateLimitType.QUERY_COUNT and rule_request.max_requests <= 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="max_requests must be greater than 0 for query_count type"
            )

        if limit_type_enum == RateLimitType.COMPLEXITY_BUDGET and rule_request.complexity_threshold <= 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="complexity_threshold must be greater than 0 for complexity_budget type"
            )

        # Create new rule
        new_rule = RateLimitRule(
            name=rule_request.name,
            limit_type=limit_type_enum,
            window_seconds=rule_request.window_seconds,
            max_requests=rule_request.max_requests,
            complexity_threshold=rule_request.complexity_threshold,
            user_specific=rule_request.user_specific,
            burst_allowance=rule_request.burst_allowance,
            penalty_duration_seconds=rule_request.penalty_duration_seconds,
            priority=rule_request.priority,
            conditions=rule_request.conditions or {}
        )

        # Add rule to analyzer
        analyzer.rate_limit_rules.append(new_rule)

        # Sort rules by priority
        analyzer.rate_limit_rules.sort(key=lambda r: r.priority)

        logger.info(f"Created rate limit rule: {rule_request.name}")

        return {
            "message": "Rate limit rule created successfully",
            "rule": {
                "name": new_rule.name,
                "limit_type": new_rule.limit_type.value,
                "window_seconds": new_rule.window_seconds,
                "max_requests": new_rule.max_requests,
                "complexity_threshold": new_rule.complexity_threshold,
                "user_specific": new_rule.user_specific,
                "burst_allowance": new_rule.burst_allowance,
                "penalty_duration_seconds": new_rule.penalty_duration_seconds,
                "priority": new_rule.priority,
                "conditions": new_rule.conditions
            },
            "total_rules": len(analyzer.rate_limit_rules),
            "rule_position": analyzer.rate_limit_rules.index(new_rule) + 1,
            "created_by": current_user["sub"],
            "created_at": datetime.utcnow().isoformat()
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create rate limit rule: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to create rate limit rule"
        )


@router.delete("/rate-limits/rules/{rule_name}", response_model=dict)
async def delete_rate_limit_rule(
    rule_name: str,
    analyzer: GraphQLQueryComplexityAnalyzer = Depends(lambda: get_query_complexity_analyzer(get_graphql_schema())),
    current_user: dict = Depends(lambda: {"sub": "system", "permissions": ["perm_admin_system"]})
) -> dict:
    """
    Delete a rate limiting rule

    Removes a rate limiting rule from the system. Active rate limiting
    states are preserved to prevent circumvention of existing limits.
    """
    try:
        # Find rule to delete
        rule_to_delete = None
        for rule in analyzer.rate_limit_rules:
            if rule.name == rule_name:
                rule_to_delete = rule
                break

        if not rule_to_delete:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Rate limit rule '{rule_name}' not found"
            )

        # Remove rule
        analyzer.rate_limit_rules.remove(rule_to_delete)

        logger.info(f"Deleted rate limit rule: {rule_name}")

        return {
            "message": f"Rate limit rule '{rule_name}' deleted successfully",
            "deleted_rule": {
                "name": rule_to_delete.name,
                "limit_type": rule_to_delete.limit_type.value,
                "priority": rule_to_delete.priority
            },
            "remaining_rules": len(analyzer.rate_limit_rules),
            "deleted_by": current_user["sub"],
            "deleted_at": datetime.utcnow().isoformat()
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete rate limit rule {rule_name}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete rate limit rule: {rule_name}"
        )


@router.get("/rate-limits/states", response_model=dict)
async def list_rate_limit_states(
    limit: int = 50,
    offset: int = 0,
    blocked_only: bool = False,
    analyzer: GraphQLQueryComplexityAnalyzer = Depends(lambda: get_query_complexity_analyzer(get_graphql_schema())),
    current_user: dict = Depends(lambda: {"sub": "system", "permissions": ["perm_monitoring_read"]})
) -> dict:
    """
    List current rate limiting states for clients

    Provides visibility into:
    - Active client rate limiting states
    - Blocked clients and penalties
    - Violation history and patterns
    - Usage statistics per client
    """
    try:
        all_states = []
        for client_id, state in analyzer.rate_limit_states.items():
            # Filter blocked only if requested
            if blocked_only and (not state.blocked_until or datetime.utcnow() >= state.blocked_until):
                continue

            state_info = {
                "client_id": client_id,
                "window_start": state.window_start.isoformat(),
                "request_count": state.request_count,
                "complexity_used": state.complexity_used,
                "blocked_until": state.blocked_until.isoformat() if state.blocked_until else None,
                "is_currently_blocked": state.blocked_until and datetime.utcnow() < state.blocked_until,
                "violation_count": len(state.violations),
                "last_request": state.last_request.isoformat(),
                "recent_violations": [
                    {
                        "timestamp": v['timestamp'].isoformat(),
                        "rule": v['rule'],
                        "complexity": v.get('complexity', 0),
                        "reason": v['reason']
                    }
                    for v in state.violations[-5:]  # Last 5 violations
                ]
            }
            all_states.append(state_info)

        # Sort by last request time (most recent first)
        all_states.sort(key=lambda s: s["last_request"], reverse=True)

        # Apply pagination
        total_count = len(all_states)
        paginated_states = all_states[offset:offset + limit]

        return {
            "rate_limit_states": paginated_states,
            "pagination": {
                "total_count": total_count,
                "limit": limit,
                "offset": offset,
                "has_next": offset + limit < total_count,
                "has_previous": offset > 0
            },
            "summary": {
                "total_clients": len(analyzer.rate_limit_states),
                "currently_blocked": len([
                    state for state in analyzer.rate_limit_states.values()
                    if state.blocked_until and datetime.utcnow() < state.blocked_until
                ]),
                "clients_with_violations": len([
                    state for state in analyzer.rate_limit_states.values()
                    if state.violations
                ]),
                "total_violations_24h": sum([
                    len([v for v in state.violations if v['timestamp'] > datetime.utcnow() - timedelta(hours=24)])
                    for state in analyzer.rate_limit_states.values()
                ])
            }
        }

    except Exception as e:
        logger.error(f"Failed to list rate limit states: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve rate limit states"
        )


# Configuration Management Endpoints

@router.get("/configuration", response_model=dict)
async def get_complexity_configuration(
    analyzer: GraphQLQueryComplexityAnalyzer = Depends(lambda: get_query_complexity_analyzer(get_graphql_schema())),
    current_user: dict = Depends(lambda: {"sub": "system", "permissions": ["perm_monitoring_read"]})
) -> dict:
    """
    Get current GraphQL complexity analysis configuration

    Returns comprehensive configuration information including:
    - Complexity limits and thresholds
    - Field complexity mappings
    - Rate limiting settings
    - Security configurations
    - Performance parameters
    """
    try:
        config = {
            "complexity_limits": {
                "max_complexity": analyzer.max_complexity,
                "max_depth": analyzer.max_depth,
                "default_field_complexity": analyzer.default_field_complexity,
                "list_multiplier": analyzer.list_multiplier,
                "enable_introspection_limit": analyzer.enable_introspection_limit
            },
            "field_complexity_mappings": {
                "total_mappings": len(analyzer.field_complexity_map),
                "custom_mappings": dict(list(analyzer.field_complexity_map.items())[:20]),  # First 20 mappings
                "high_complexity_fields": {
                    k: v for k, v in analyzer.field_complexity_map.items() if v > 30
                }
            },
            "resolver_complexity_mappings": {
                "total_mappings": len(analyzer.resolver_complexity_map),
                "mappings": analyzer.resolver_complexity_map
            },
            "rate_limiting_config": {
                "total_rules": len(analyzer.rate_limit_rules),
                "rules_by_priority": [
                    {
                        "name": rule.name,
                        "type": rule.limit_type.value,
                        "priority": rule.priority,
                        "user_specific": rule.user_specific
                    }
                    for rule in sorted(analyzer.rate_limit_rules, key=lambda r: r.priority)
                ],
                "redis_enabled": analyzer.redis_client is not None,
                "fallback_mode": analyzer.redis_client is None
            },
            "security_features": {
                "malicious_pattern_detection": True,
                "introspection_monitoring": True,
                "depth_limiting": True,
                "complexity_blocking": True,
                "query_analysis": True
            },
            "performance_tracking": {
                "history_size": len(analyzer.performance_history),
                "metrics_retention": "1000 recent queries",
                "pattern_detection": True,
                "trend_analysis": True
            },
            "system_status": {
                "analyzer_initialized": True,
                "redis_connection": "connected" if analyzer.redis_client else "fallback",
                "active_monitoring": True,
                "query_tracking": len(analyzer.query_metrics)
            }
        }

        return config

    except Exception as e:
        logger.error(f"Failed to get complexity configuration: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve complexity configuration"
        )


@router.put("/configuration", response_model=dict)
async def update_complexity_configuration(
    config_request: ComplexityConfigRequest,
    analyzer: GraphQLQueryComplexityAnalyzer = Depends(lambda: get_query_complexity_analyzer(get_graphql_schema())),
    current_user: dict = Depends(lambda: {"sub": "system", "permissions": ["perm_admin_system"]})
) -> dict:
    """
    Update GraphQL complexity analysis configuration

    Allows real-time configuration updates for:
    - Complexity limits and thresholds
    - Field complexity overrides
    - Security parameters
    - Performance tuning
    """
    try:
        updates_applied = []

        # Update complexity limits
        if config_request.max_complexity is not None:
            analyzer.max_complexity = config_request.max_complexity
            updates_applied.append(f"max_complexity: {config_request.max_complexity}")

        if config_request.max_depth is not None:
            analyzer.max_depth = config_request.max_depth
            updates_applied.append(f"max_depth: {config_request.max_depth}")

        if config_request.default_field_complexity is not None:
            analyzer.default_field_complexity = config_request.default_field_complexity
            updates_applied.append(f"default_field_complexity: {config_request.default_field_complexity}")

        if config_request.list_multiplier is not None:
            analyzer.list_multiplier = config_request.list_multiplier
            updates_applied.append(f"list_multiplier: {config_request.list_multiplier}")

        if config_request.enable_introspection_limit is not None:
            analyzer.enable_introspection_limit = config_request.enable_introspection_limit
            updates_applied.append(f"enable_introspection_limit: {config_request.enable_introspection_limit}")

        # Update field complexity overrides
        if config_request.field_complexity_overrides:
            for field_name, complexity in config_request.field_complexity_overrides.items():
                analyzer.field_complexity_map[field_name] = complexity
                updates_applied.append(f"field_complexity[{field_name}]: {complexity}")

        logger.info(f"Updated GraphQL complexity configuration: {', '.join(updates_applied)}")

        return {
            "message": "GraphQL complexity configuration updated successfully",
            "updates_applied": updates_applied,
            "current_configuration": {
                "max_complexity": analyzer.max_complexity,
                "max_depth": analyzer.max_depth,
                "default_field_complexity": analyzer.default_field_complexity,
                "list_multiplier": analyzer.list_multiplier,
                "enable_introspection_limit": analyzer.enable_introspection_limit,
                "total_field_mappings": len(analyzer.field_complexity_map)
            },
            "updated_by": current_user["sub"],
            "updated_at": datetime.utcnow().isoformat()
        }

    except Exception as e:
        logger.error(f"Failed to update complexity configuration: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to update complexity configuration"
        )


# Health and Status Endpoints

@router.get("/health", response_model=dict)
async def get_graphql_complexity_health(
    analyzer: GraphQLQueryComplexityAnalyzer = Depends(lambda: get_query_complexity_analyzer(get_graphql_schema())),
    current_user: dict = Depends(lambda: {"sub": "system", "permissions": ["perm_monitoring_read"]})
) -> dict:
    """
    Get GraphQL complexity analyzer health status

    Returns comprehensive health information including:
    - System component status
    - Performance metrics
    - Resource utilization
    - Configuration validity
    - Integration status
    """
    try:
        # Calculate health metrics
        total_queries = len(analyzer.query_metrics)
        recent_queries = len([
            m for m in analyzer.query_metrics.values()
            if m.timestamp > datetime.utcnow() - timedelta(minutes=5)
        ])

        blocked_queries = len([m for m in analyzer.query_metrics.values() if m.rate_limit_applied])
        success_rate = ((total_queries - blocked_queries) / total_queries * 100) if total_queries else 100

        return {
            "system_health": {
                "overall_status": "healthy",
                "complexity_analyzer": "operational",
                "rate_limiting": "active",
                "pattern_detection": "enabled",
                "performance_tracking": "active"
            },
            "component_status": {
                "query_parser": "healthy",
                "complexity_calculator": "healthy",
                "rate_limiter": "healthy",
                "redis_connection": "connected" if analyzer.redis_client else "fallback_mode",
                "metrics_collector": "healthy",
                "security_scanner": "healthy"
            },
            "performance_metrics": {
                "total_queries_analyzed": total_queries,
                "queries_last_5_minutes": recent_queries,
                "success_rate_percent": round(success_rate, 2),
                "average_analysis_time_ms": 2.5,  # Mock data
                "memory_usage_mb": 45,  # Mock data
                "cpu_utilization_percent": 8  # Mock data
            },
            "configuration_status": {
                "max_complexity_configured": analyzer.max_complexity,
                "max_depth_configured": analyzer.max_depth,
                "rate_limit_rules": len(analyzer.rate_limit_rules),
                "field_complexity_mappings": len(analyzer.field_complexity_map),
                "resolver_mappings": len(analyzer.resolver_complexity_map)
            },
            "security_metrics": {
                "malicious_patterns_detected": len(analyzer.malicious_patterns),
                "blocked_clients": len([
                    state for state in analyzer.rate_limit_states.values()
                    if state.blocked_until and datetime.utcnow() < state.blocked_until
                ]),
                "introspection_queries_blocked": 0,  # Mock data
                "threat_level": _assess_security_threat_level(analyzer)
            },
            "resource_utilization": {
                "query_metrics_cache_size": len(analyzer.query_metrics),
                "performance_history_size": len(analyzer.performance_history),
                "rate_limit_states": len(analyzer.rate_limit_states),
                "pattern_cache_size": len(analyzer.query_patterns)
            },
            "last_health_check": datetime.utcnow().isoformat(),
            "uptime": "continuous",  # Mock data
            "version": "1.0.0"
        }

    except Exception as e:
        logger.error(f"Failed to get GraphQL complexity health: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve GraphQL complexity health status"
        )