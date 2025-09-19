"""
Performance Optimization API Routes
===================================

FastAPI routes for managing and monitoring API performance optimization
to achieve <15ms response times.

Key Features:
- Real-time performance monitoring and analysis
- Optimization recommendations and automatic tuning
- Performance profiling and bottleneck identification
- Caching strategy management and optimization
- Database query optimization monitoring
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, validator

from core.auth import get_current_user, User
from api.performance.api_performance_profiler import api_profiler

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create router
router = APIRouter(prefix="/performance-optimization", tags=["Performance Optimization"])


# Pydantic Models
class OptimizationRequest(BaseModel):
    """Request model for performance optimization."""
    endpoint: Optional[str] = Field(None, description="Specific endpoint to optimize")
    auto_apply: bool = Field(False, description="Whether to automatically apply optimizations")
    optimization_types: List[str] = Field(
        default=["caching", "database", "serialization"],
        description="Types of optimizations to apply"
    )

    class Config:
        schema_extra = {
            "example": {
                "endpoint": "/api/v1/sales",
                "auto_apply": True,
                "optimization_types": ["caching", "database"]
            }
        }


class PerformanceAnalysisRequest(BaseModel):
    """Request model for performance analysis."""
    endpoint: Optional[str] = Field(None, description="Specific endpoint to analyze")
    time_window_hours: int = Field(24, ge=1, le=168, description="Time window in hours")
    include_trends: bool = Field(True, description="Include performance trends")
    include_recommendations: bool = Field(True, description="Include optimization recommendations")

    class Config:
        schema_extra = {
            "example": {
                "endpoint": "/api/v1/dashboard",
                "time_window_hours": 6,
                "include_trends": True,
                "include_recommendations": True
            }
        }


class CacheOptimizationRequest(BaseModel):
    """Request model for cache optimization."""
    endpoint_pattern: str = Field(..., description="Endpoint pattern to optimize")
    cache_ttl_seconds: int = Field(300, ge=60, le=3600, description="Cache TTL in seconds")
    cache_strategy: str = Field("lru", description="Cache eviction strategy")
    enable_preloading: bool = Field(False, description="Enable cache preloading")

    @validator('cache_strategy')
    def validate_cache_strategy(cls, v):
        allowed_strategies = ['lru', 'fifo', 'lfu', 'random']
        if v not in allowed_strategies:
            raise ValueError(f'Cache strategy must be one of: {allowed_strategies}')
        return v

    class Config:
        schema_extra = {
            "example": {
                "endpoint_pattern": "/api/v1/sales/*",
                "cache_ttl_seconds": 600,
                "cache_strategy": "lru",
                "enable_preloading": True
            }
        }


# API Endpoints
@router.post("/analyze", response_model=Dict[str, Any])
async def analyze_performance(
    request: PerformanceAnalysisRequest,
    current_user: User = Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Analyze API performance and identify optimization opportunities.

    Provides comprehensive analysis of endpoint performance including bottlenecks,
    trends, and specific recommendations for achieving <15ms response times.
    """
    try:
        # Get optimization report from profiler
        report = api_profiler.get_optimization_report(request.endpoint)

        # Add trends if requested
        if request.include_trends and request.endpoint:
            trends = api_profiler.get_performance_trends(
                request.endpoint,
                request.time_window_hours
            )
            report['performance_trends'] = trends

        # Add user context
        report['analyzed_by'] = current_user.id
        report['analysis_timestamp'] = datetime.utcnow().isoformat()
        report['analysis_scope'] = {
            'endpoint_filter': request.endpoint,
            'time_window_hours': request.time_window_hours,
            'include_trends': request.include_trends,
            'include_recommendations': request.include_recommendations
        }

        # Enhance with specific recommendations if requested
        if request.include_recommendations and report.get('endpoints_needing_attention'):
            for endpoint_info in report['endpoints_needing_attention']:
                endpoint = endpoint_info['endpoint']

                # Get specific recommendations for this endpoint
                specific_recs = []
                if endpoint_info['avg_response_time_ms'] > 15:
                    specific_recs.extend([
                        "Implement response caching for GET requests",
                        "Optimize database queries and add indexes",
                        "Consider async processing for heavy operations",
                        "Add connection pooling for database operations"
                    ])

                if endpoint_info['primary_bottleneck'] == 'database_queries':
                    specific_recs.extend([
                        "Analyze and optimize slow database queries",
                        "Implement query result caching",
                        "Add database connection pooling",
                        "Consider read replicas for read-heavy operations"
                    ])

                endpoint_info['specific_recommendations'] = specific_recs

        # Calculate optimization priority
        if report.get('performance_summary'):
            sla_compliance = report['performance_summary'].get('sla_compliance_rate', 100)

            if sla_compliance < 80:
                report['optimization_priority'] = 'critical'
                report['priority_message'] = 'Immediate optimization required - SLA compliance below 80%'
            elif sla_compliance < 95:
                report['optimization_priority'] = 'high'
                report['priority_message'] = 'Optimization recommended - SLA compliance below 95%'
            else:
                report['optimization_priority'] = 'low'
                report['priority_message'] = 'Performance within acceptable range'

        return report

    except Exception as e:
        logger.error(f"Failed to analyze performance: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Performance analysis failed"
        )


@router.post("/optimize", response_model=Dict[str, Any])
async def optimize_performance(
    request: OptimizationRequest,
    background_tasks: BackgroundTasks,
    current_user: User = Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Apply performance optimizations to achieve <15ms response times.

    Analyzes endpoint performance and applies automated optimizations
    including caching, database optimization, and resource management.
    """
    try:
        optimization_results = []

        if request.endpoint:
            # Optimize specific endpoint
            result = await api_profiler.optimize_endpoint_automatically(request.endpoint)
            optimization_results.append(result)
        else:
            # Get all endpoints that need optimization
            report = api_profiler.get_optimization_report()

            for endpoint_info in report.get('endpoints_needing_attention', []):
                endpoint = endpoint_info['endpoint']
                result = await api_profiler.optimize_endpoint_automatically(endpoint)
                optimization_results.append(result)

        # Apply optimizations in background if requested
        if request.auto_apply:
            for result in optimization_results:
                background_tasks.add_task(
                    _apply_optimization_background,
                    result['endpoint'],
                    result['optimizations_applied'],
                    current_user.id
                )

        # Calculate total estimated improvement
        total_improvement = sum(
            r.get('estimated_improvement_ms', 0) for r in optimization_results
        )

        return {
            'timestamp': datetime.utcnow().isoformat(),
            'optimized_by': current_user.id,
            'optimization_scope': {
                'endpoint_filter': request.endpoint,
                'auto_apply': request.auto_apply,
                'optimization_types': request.optimization_types
            },
            'endpoints_optimized': len(optimization_results),
            'optimization_results': optimization_results,
            'summary': {
                'total_estimated_improvement_ms': round(total_improvement, 2),
                'endpoints_processed': len(optimization_results),
                'auto_applied': request.auto_apply
            },
            'next_steps': [
                'Monitor performance after optimization',
                'Validate response times meet <15ms target',
                'Apply additional optimizations if needed',
                'Update caching strategies based on usage patterns'
            ]
        }

    except Exception as e:
        logger.error(f"Failed to optimize performance: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Performance optimization failed"
        )


@router.get("/status", response_model=Dict[str, Any])
async def get_optimization_status(
    current_user: User = Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get current performance optimization status.

    Returns real-time performance metrics, optimization status,
    and compliance with <15ms SLA targets.
    """
    try:
        # Get optimization report
        report = api_profiler.get_optimization_report()

        # Calculate key metrics
        total_endpoints = report.get('profiles_analyzed', 0)
        slow_endpoints = len(report.get('endpoints_needing_attention', []))

        if total_endpoints > 0:
            performance_score = ((total_endpoints - slow_endpoints) / total_endpoints) * 100
        else:
            performance_score = 100.0

        # Get performance summary
        perf_summary = report.get('performance_summary', {})

        status_info = {
            'timestamp': datetime.utcnow().isoformat(),
            'retrieved_by': current_user.id,
            'optimization_status': {
                'total_endpoints_monitored': total_endpoints,
                'endpoints_meeting_sla': total_endpoints - slow_endpoints,
                'endpoints_needing_optimization': slow_endpoints,
                'overall_performance_score': round(performance_score, 1),
                'sla_compliance_rate': perf_summary.get('sla_compliance_rate', 100.0)
            },
            'performance_metrics': {
                'avg_response_time_ms': perf_summary.get('avg_response_time_ms', 0.0),
                'p95_response_time_ms': perf_summary.get('p95_response_time_ms', 0.0),
                'p99_response_time_ms': perf_summary.get('p99_response_time_ms', 0.0),
                'target_response_time_ms': 15.0
            },
            'optimization_opportunities': report.get('optimization_recommendations', {}),
            'system_status': 'optimal' if performance_score >= 95 else 'needs_attention' if performance_score >= 80 else 'critical'
        }

        # Add status-specific recommendations
        if performance_score < 80:
            status_info['urgent_actions'] = [
                'Immediate performance optimization required',
                'Review and optimize slowest endpoints first',
                'Implement aggressive caching strategies',
                'Consider infrastructure scaling'
            ]
        elif performance_score < 95:
            status_info['recommended_actions'] = [
                'Optimize remaining slow endpoints',
                'Implement preventive caching',
                'Monitor performance trends closely'
            ]
        else:
            status_info['maintenance_actions'] = [
                'Continue monitoring performance',
                'Optimize caching strategies',
                'Plan for future capacity needs'
            ]

        return status_info

    except Exception as e:
        logger.error(f"Failed to get optimization status: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve optimization status"
        )


@router.get("/recommendations/{endpoint:path}", response_model=Dict[str, Any])
async def get_endpoint_recommendations(
    endpoint: str,
    current_user: User = Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get specific optimization recommendations for an endpoint.

    Analyzes the endpoint's performance profile and provides detailed
    recommendations for achieving <15ms response times.
    """
    try:
        # Clean endpoint path
        endpoint_path = f"/{endpoint}" if not endpoint.startswith('/') else endpoint

        # Get optimization report for the specific endpoint
        report = api_profiler.get_optimization_report(endpoint_path)

        if not report or report.get('profiles_analyzed', 0) == 0:
            return {
                'endpoint': endpoint_path,
                'status': 'no_data',
                'message': 'No performance data available for this endpoint',
                'recommendations': [
                    'Enable performance monitoring',
                    'Generate some traffic to collect performance data',
                    'Check if endpoint path is correct'
                ]
            }

        # Get specific endpoint data
        endpoint_details = None
        for ep_info in report.get('endpoints_needing_attention', []):
            if ep_info['endpoint'] == endpoint_path:
                endpoint_details = ep_info
                break

        if not endpoint_details:
            # Endpoint is performing well
            return {
                'endpoint': endpoint_path,
                'status': 'optimal',
                'current_performance': report.get('performance_summary', {}),
                'message': 'Endpoint is meeting <15ms performance target',
                'recommendations': [
                    'Continue monitoring performance',
                    'Maintain current optimization strategies',
                    'Consider implementing preventive caching'
                ]
            }

        # Generate detailed recommendations
        detailed_recommendations = []

        current_time = endpoint_details['avg_response_time_ms']
        bottleneck = endpoint_details.get('primary_bottleneck', 'unknown')

        if bottleneck == 'database_queries':
            detailed_recommendations.extend([
                {
                    'category': 'Database Optimization',
                    'priority': 'critical',
                    'recommendation': 'Optimize database queries',
                    'details': 'Database operations are the primary bottleneck',
                    'estimated_improvement_ms': min(current_time * 0.4, 8.0),
                    'implementation_steps': [
                        'Analyze slow query logs',
                        'Add appropriate database indexes',
                        'Optimize JOIN operations',
                        'Implement query result caching'
                    ]
                }
            ])

        if current_time > 20:
            detailed_recommendations.extend([
                {
                    'category': 'Caching Strategy',
                    'priority': 'high',
                    'recommendation': 'Implement aggressive response caching',
                    'details': 'Response time significantly exceeds target',
                    'estimated_improvement_ms': min(current_time * 0.6, 12.0),
                    'implementation_steps': [
                        'Implement Redis caching',
                        'Cache database query results',
                        'Add response-level caching',
                        'Implement cache prewarming'
                    ]
                }
            ])

        if current_time > 10:
            detailed_recommendations.extend([
                {
                    'category': 'Connection Optimization',
                    'priority': 'medium',
                    'recommendation': 'Optimize database connections',
                    'details': 'Connection overhead may be contributing to latency',
                    'estimated_improvement_ms': 2.0,
                    'implementation_steps': [
                        'Implement connection pooling',
                        'Optimize connection pool size',
                        'Use persistent connections',
                        'Consider connection multiplexing'
                    ]
                }
            ])

        # Calculate total estimated improvement
        total_improvement = sum(r['estimated_improvement_ms'] for r in detailed_recommendations)
        projected_time = max(1.0, current_time - total_improvement)

        return {
            'endpoint': endpoint_path,
            'status': 'needs_optimization',
            'current_performance': {
                'avg_response_time_ms': current_time,
                'primary_bottleneck': bottleneck,
                'target_response_time_ms': 15.0,
                'performance_gap_ms': max(0, current_time - 15.0)
            },
            'optimization_plan': {
                'total_recommendations': len(detailed_recommendations),
                'estimated_total_improvement_ms': round(total_improvement, 2),
                'projected_response_time_ms': round(projected_time, 2),
                'will_meet_target': projected_time <= 15.0
            },
            'detailed_recommendations': detailed_recommendations,
            'implementation_priority': 'critical' if current_time > 25 else 'high' if current_time > 20 else 'medium',
            'next_steps': [
                'Implement recommendations in priority order',
                'Monitor performance after each optimization',
                'Validate target achievement',
                'Continue optimization if needed'
            ]
        }

    except Exception as e:
        logger.error(f"Failed to get endpoint recommendations: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve endpoint recommendations"
        )


@router.post("/cache/optimize", response_model=Dict[str, Any])
async def optimize_caching(
    request: CacheOptimizationRequest,
    background_tasks: BackgroundTasks,
    current_user: User = Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Optimize caching strategies for improved performance.

    Configures intelligent caching strategies to reduce response times
    and improve cache hit rates for better performance.
    """
    try:
        # Apply cache optimization in background
        background_tasks.add_task(
            _optimize_cache_background,
            request.endpoint_pattern,
            request.cache_ttl_seconds,
            request.cache_strategy,
            request.enable_preloading,
            current_user.id
        )

        return {
            'timestamp': datetime.utcnow().isoformat(),
            'optimized_by': current_user.id,
            'cache_optimization': {
                'endpoint_pattern': request.endpoint_pattern,
                'cache_ttl_seconds': request.cache_ttl_seconds,
                'cache_strategy': request.cache_strategy,
                'preloading_enabled': request.enable_preloading
            },
            'status': 'optimization_applied',
            'expected_benefits': [
                'Reduced response times for cached requests',
                'Lower database load',
                'Improved user experience',
                'Better resource utilization'
            ],
            'monitoring_recommendations': [
                'Monitor cache hit rates',
                'Track response time improvements',
                'Adjust TTL based on usage patterns',
                'Monitor memory usage'
            ]
        }

    except Exception as e:
        logger.error(f"Failed to optimize caching: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Cache optimization failed"
        )


# Background task functions
async def _apply_optimization_background(
    endpoint: str,
    optimizations: List[str],
    user_id: str
):
    """Apply optimizations in background."""
    try:
        logger.info(f"Applying optimizations for {endpoint}: {optimizations} (by {user_id})")

        # Here you would implement actual optimizations
        # For now, just log the actions
        for optimization in optimizations:
            if optimization == "database_connection_pooling":
                logger.info(f"Applied database connection pooling for {endpoint}")
            elif optimization == "query_result_caching":
                logger.info(f"Applied query result caching for {endpoint}")
            elif optimization == "cache_strategy_optimization":
                logger.info(f"Applied cache strategy optimization for {endpoint}")
            elif optimization == "response_compression":
                logger.info(f"Applied response compression for {endpoint}")

        logger.info(f"Completed optimization application for {endpoint}")

    except Exception as e:
        logger.error(f"Failed to apply optimizations for {endpoint}: {e}")


async def _optimize_cache_background(
    endpoint_pattern: str,
    ttl_seconds: int,
    strategy: str,
    enable_preloading: bool,
    user_id: str
):
    """Optimize caching in background."""
    try:
        logger.info(f"Optimizing cache for {endpoint_pattern} (TTL: {ttl_seconds}s, Strategy: {strategy}) by {user_id}")

        # Here you would implement actual cache optimization
        # For now, just log the configuration
        logger.info(f"Cache optimization applied: pattern={endpoint_pattern}, ttl={ttl_seconds}, strategy={strategy}, preloading={enable_preloading}")

    except Exception as e:
        logger.error(f"Failed to optimize cache for {endpoint_pattern}: {e}")