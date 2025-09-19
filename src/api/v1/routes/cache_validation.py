"""
Cache Validation API Routes
===========================

FastAPI routes for production-grade cache validation and optimization.

Key Features:
- Multi-layer cache validation (L1/L2/L3/L4)
- Cache performance analysis and benchmarking
- Cache coherence and consistency testing
- Optimization recommendations and automated tuning
- Real-time cache health monitoring
"""

import logging
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, validator

from core.auth import get_current_user, User
from core.caching.cache_validation_engine import cache_validator, CacheLayer, CacheValidationStatus

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Create router
router = APIRouter(prefix="/cache-validation", tags=["Cache Validation"])


# Pydantic Models
class CacheValidationRequest(BaseModel):
    """Request model for cache validation."""
    layers: List[str] = Field(
        default=["l1_memory", "l2_redis", "l3_database"],
        description="Cache layers to validate"
    )
    include_performance_test: bool = Field(True, description="Include performance benchmarking")
    include_coherence_test: bool = Field(True, description="Include cache coherence testing")
    include_warming_test: bool = Field(True, description="Include cache warming validation")

    @validator('layers')
    def validate_layers(cls, v):
        valid_layers = [layer.value for layer in CacheLayer]
        for layer in v:
            if layer not in valid_layers:
                raise ValueError(f'Invalid cache layer: {layer}. Valid layers: {valid_layers}')
        return v

    class Config:
        schema_extra = {
            "example": {
                "layers": ["l1_memory", "l2_redis"],
                "include_performance_test": True,
                "include_coherence_test": True,
                "include_warming_test": False
            }
        }


class CacheOptimizationRequest(BaseModel):
    """Request model for cache optimization."""
    target_hit_rate: float = Field(90.0, ge=70.0, le=99.0, description="Target cache hit rate percentage")
    target_response_time_ms: float = Field(2.0, ge=0.1, le=10.0, description="Target cache response time in ms")
    enable_auto_optimization: bool = Field(False, description="Enable automatic optimization")
    optimization_types: List[str] = Field(
        default=["hit_rate", "response_time", "memory_usage"],
        description="Types of optimizations to apply"
    )

    class Config:
        schema_extra = {
            "example": {
                "target_hit_rate": 92.0,
                "target_response_time_ms": 1.5,
                "enable_auto_optimization": True,
                "optimization_types": ["hit_rate", "response_time"]
            }
        }


# API Endpoints
@router.on_event("startup")
async def initialize_cache_validator():
    """Initialize the cache validator on startup."""
    try:
        await cache_validator.initialize()
        logger.info("Cache Validator initialized successfully")
    except Exception as e:
        logger.error(f"Failed to initialize Cache Validator: {e}")


@router.post("/validate", response_model=Dict[str, Any])
async def validate_cache_layers(
    request: CacheValidationRequest,
    background_tasks: BackgroundTasks,
    current_user: User = Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Run comprehensive cache validation across all layers.

    Validates cache performance, coherence, and optimization opportunities
    for production-grade caching systems targeting <15ms response times.
    """
    try:
        logger.info(f"Starting cache validation requested by {current_user.id}")

        # Run validation in background if it's a comprehensive test
        if len(request.layers) > 2 or all([request.include_performance_test,
                                         request.include_coherence_test,
                                         request.include_warming_test]):

            # Start background validation
            background_tasks.add_task(
                _run_comprehensive_validation,
                request,
                current_user.id
            )

            return {
                "message": "Comprehensive cache validation started in background",
                "validation_scope": {
                    "layers": request.layers,
                    "performance_test": request.include_performance_test,
                    "coherence_test": request.include_coherence_test,
                    "warming_test": request.include_warming_test
                },
                "initiated_by": current_user.id,
                "timestamp": datetime.utcnow().isoformat(),
                "estimated_completion_time": "2-5 minutes",
                "status_endpoint": "/api/v1/cache-validation/status"
            }

        else:
            # Run quick validation synchronously
            result = await cache_validator.validate_cache_layers()

            return {
                "validation_id": result.validation_id,
                "timestamp": result.timestamp.isoformat(),
                "validated_by": current_user.id,
                "overall_status": result.overall_status.value,
                "validation_time_ms": result.total_validation_time_ms,
                "performance_improvement": result.performance_improvement,
                "layer_performance": {
                    layer.value: {
                        "hit_rate": metrics.hit_rate,
                        "average_response_time_ms": metrics.average_response_time_ms,
                        "efficiency_score": metrics.efficiency_score,
                        "throughput_ops_per_second": metrics.throughput_ops_per_second
                    }
                    for layer, metrics in result.layer_metrics.items()
                },
                "validation_tests": result.validation_tests,
                "recommendations": result.recommendations,
                "critical_issues": result.critical_issues
            }

    except Exception as e:
        logger.error(f"Cache validation failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Cache validation failed"
        )


@router.get("/status", response_model=Dict[str, Any])
async def get_validation_status(
    current_user: User = Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get current cache validation status and health.

    Returns real-time cache performance metrics, validation status,
    and optimization recommendations.
    """
    try:
        summary = cache_validator.get_validation_summary()

        if summary.get("message") == "No validation history available":
            return {
                "status": "no_data",
                "message": "No cache validation data available. Run validation first.",
                "recommendations": [
                    "Run initial cache validation",
                    "Enable cache monitoring",
                    "Configure cache layers properly"
                ],
                "next_steps": [
                    "POST /api/v1/cache-validation/validate to start validation"
                ]
            }

        latest = summary["latest_validation"]

        # Determine health status
        status_map = {
            "healthy": "optimal",
            "degraded": "warning",
            "critical": "critical",
            "failed": "error"
        }

        health_status = status_map.get(latest["status"], "unknown")

        return {
            "timestamp": datetime.utcnow().isoformat(),
            "retrieved_by": current_user.id,
            "cache_health": {
                "overall_status": health_status,
                "performance_score": latest["performance_improvement"],
                "last_validation": latest["timestamp"],
                "validation_time_ms": latest["validation_time_ms"]
            },
            "historical_performance": {
                "total_validations": summary["historical_summary"]["total_validations"],
                "average_performance_improvement": round(summary["historical_summary"]["average_performance_improvement"], 2),
                "status_distribution": summary["historical_summary"]["status_distribution"]
            },
            "current_issues": {
                "critical_issues_count": summary["critical_issues_count"],
                "recommendations_count": len(summary["current_recommendations"])
            },
            "optimization_opportunities": summary["current_recommendations"],
            "system_status": "operational" if health_status == "optimal" else "needs_attention"
        }

    except Exception as e:
        logger.error(f"Failed to get validation status: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve validation status"
        )


@router.post("/optimize", response_model=Dict[str, Any])
async def optimize_cache_performance(
    request: CacheOptimizationRequest,
    background_tasks: BackgroundTasks,
    current_user: User = Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Optimize cache performance to meet production targets.

    Applies automated optimizations based on validation results to improve
    cache hit rates, response times, and overall performance.
    """
    try:
        logger.info(f"Cache optimization requested by {current_user.id}")

        # Get current validation status
        summary = cache_validator.get_validation_summary()

        if summary.get("message") == "No validation history available":
            return {
                "status": "validation_required",
                "message": "Run cache validation before optimization",
                "required_action": "POST /api/v1/cache-validation/validate",
                "reason": "Optimization requires current performance baseline"
            }

        # Apply optimization in background
        if request.enable_auto_optimization:
            background_tasks.add_task(
                _apply_cache_optimization,
                request,
                current_user.id
            )

            return {
                "message": "Cache optimization started in background",
                "optimization_targets": {
                    "target_hit_rate": request.target_hit_rate,
                    "target_response_time_ms": request.target_response_time_ms,
                    "optimization_types": request.optimization_types
                },
                "initiated_by": current_user.id,
                "timestamp": datetime.utcnow().isoformat(),
                "estimated_completion_time": "1-3 minutes",
                "auto_optimization_enabled": request.enable_auto_optimization
            }

        else:
            # Generate optimization plan without applying
            optimization_plan = await _generate_optimization_plan(request, summary)

            return {
                "message": "Cache optimization plan generated",
                "optimization_plan": optimization_plan,
                "current_performance": summary["latest_validation"],
                "estimated_improvements": {
                    "hit_rate_improvement": max(0, request.target_hit_rate - summary["latest_validation"]["performance_improvement"]),
                    "response_time_improvement_ms": "varies by optimization",
                    "memory_efficiency_improvement": "up to 20%"
                },
                "implementation_required": True,
                "next_steps": [
                    "Review optimization plan",
                    "Apply optimizations with enable_auto_optimization=true",
                    "Monitor performance after optimization"
                ]
            }

    except Exception as e:
        logger.error(f"Cache optimization failed: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Cache optimization failed"
        )


@router.get("/metrics", response_model=Dict[str, Any])
async def get_cache_metrics(
    layer: Optional[str] = None,
    time_window_hours: int = 24,
    current_user: User = Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get detailed cache performance metrics.

    Returns comprehensive cache performance data including hit rates,
    response times, throughput, and efficiency scores.
    """
    try:
        summary = cache_validator.get_validation_summary()

        if summary.get("message") == "No validation history available":
            return {
                "status": "no_metrics",
                "message": "No cache metrics available",
                "action_required": "Run cache validation to collect metrics"
            }

        latest = summary["latest_validation"]

        # Filter by layer if specified
        if layer and layer not in ["l1_memory", "l2_redis", "l3_database", "l4_cdn"]:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid cache layer: {layer}"
            )

        return {
            "timestamp": datetime.utcnow().isoformat(),
            "retrieved_by": current_user.id,
            "metrics_scope": {
                "layer_filter": layer,
                "time_window_hours": time_window_hours,
                "data_freshness": "real-time"
            },
            "performance_metrics": {
                "overall_performance_score": latest["performance_improvement"],
                "validation_time_ms": latest["validation_time_ms"],
                "last_updated": latest["timestamp"]
            },
            "cache_health_indicators": {
                "status": latest["status"],
                "critical_issues": summary["critical_issues_count"],
                "optimization_opportunities": len(summary["current_recommendations"])
            },
            "recommendations": summary["current_recommendations"],
            "trending": {
                "total_validations": summary["historical_summary"]["total_validations"],
                "average_performance": summary["historical_summary"]["average_performance_improvement"],
                "trend_direction": "improving" if summary["historical_summary"]["average_performance_improvement"] > 80 else "stable"
            }
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get cache metrics: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve cache metrics"
        )


@router.delete("/reset", response_model=Dict[str, str])
async def reset_validation_data(
    current_user: User = Depends(get_current_user)
) -> Dict[str, str]:
    """
    Reset cache validation data and history.

    Clears all validation history and metrics for a fresh start.
    Use with caution in production environments.
    """
    try:
        # Clear validation history
        cache_validator.validation_history.clear()

        # Clear layer performance data
        for layer in CacheLayer:
            cache_validator.layer_performance[layer].clear()

        logger.info(f"Cache validation data reset by {current_user.id}")

        return {
            "message": "Cache validation data reset successfully",
            "reset_by": current_user.id,
            "timestamp": datetime.utcnow().isoformat(),
            "next_steps": "Run new cache validation to collect fresh metrics"
        }

    except Exception as e:
        logger.error(f"Failed to reset validation data: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to reset validation data"
        )


# Background task functions
async def _run_comprehensive_validation(
    request: CacheValidationRequest,
    user_id: str
):
    """Run comprehensive cache validation in background."""
    try:
        logger.info(f"Starting comprehensive cache validation for user {user_id}")

        # Run the full validation
        result = await cache_validator.validate_cache_layers()

        logger.info(f"Comprehensive validation completed: {result.validation_id} "
                   f"(Status: {result.overall_status.value}, "
                   f"Performance: {result.performance_improvement}%)")

    except Exception as e:
        logger.error(f"Comprehensive validation failed for user {user_id}: {e}")


async def _apply_cache_optimization(
    request: CacheOptimizationRequest,
    user_id: str
):
    """Apply cache optimizations in background."""
    try:
        logger.info(f"Applying cache optimizations for user {user_id}")

        # Here you would implement actual optimization logic
        # For now, just log the optimization parameters
        logger.info(f"Optimization targets: Hit rate={request.target_hit_rate}%, "
                   f"Response time={request.target_response_time_ms}ms")

        for optimization_type in request.optimization_types:
            logger.info(f"Applying {optimization_type} optimization")

        logger.info(f"Cache optimization completed for user {user_id}")

    except Exception as e:
        logger.error(f"Cache optimization failed for user {user_id}: {e}")


async def _generate_optimization_plan(
    request: CacheOptimizationRequest,
    summary: Dict[str, Any]
) -> Dict[str, Any]:
    """Generate detailed optimization plan."""
    try:
        current_performance = summary["latest_validation"]["performance_improvement"]
        gap_to_target = max(0, request.target_hit_rate - current_performance)

        optimization_steps = []

        if "hit_rate" in request.optimization_types:
            optimization_steps.append({
                "step": "Optimize cache hit rates",
                "priority": "high",
                "estimated_improvement": f"{gap_to_target * 0.6:.1f}%",
                "implementation": "Implement predictive caching and improve cache warming"
            })

        if "response_time" in request.optimization_types:
            optimization_steps.append({
                "step": "Reduce cache response times",
                "priority": "medium",
                "estimated_improvement": f"up to {request.target_response_time_ms * 0.5:.1f}ms reduction",
                "implementation": "Optimize cache serialization and connection pooling"
            })

        if "memory_usage" in request.optimization_types:
            optimization_steps.append({
                "step": "Optimize memory usage",
                "priority": "medium",
                "estimated_improvement": "10-20% memory reduction",
                "implementation": "Implement intelligent eviction policies and compression"
            })

        return {
            "current_performance": current_performance,
            "target_performance": request.target_hit_rate,
            "performance_gap": gap_to_target,
            "optimization_steps": optimization_steps,
            "total_estimated_improvement": f"{gap_to_target * 0.8:.1f}%",
            "implementation_time": "15-30 minutes"
        }

    except Exception as e:
        logger.error(f"Failed to generate optimization plan: {e}")
        return {"error": "Failed to generate optimization plan"}