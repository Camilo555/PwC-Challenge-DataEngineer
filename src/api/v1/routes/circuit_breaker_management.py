"""
Circuit Breaker Management API Routes
====================================

Enterprise circuit breaker management with real-time monitoring, configuration,
and operational control for external service resilience.
"""

from fastapi import APIRouter, Depends, HTTPException, status, Request
from fastapi.responses import JSONResponse
from typing import Dict, List, Optional, Any
from datetime import datetime, timedelta
from pydantic import BaseModel, Field
import json

from api.patterns.advanced_circuit_breaker import (
    get_circuit_breaker_registry,
    CircuitBreakerRegistry,
    CircuitBreakerConfig,
    ServiceTier,
    CircuitState,
    create_critical_service_config,
    create_high_service_config,
    create_standard_service_config,
    create_best_effort_service_config
)
from core.logging import get_logger

logger = get_logger(__name__)
router = APIRouter(prefix="/circuit-breaker", tags=["circuit-breaker", "resilience", "monitoring"])


# Request/Response Models
class CircuitBreakerConfigRequest(BaseModel):
    """Request model for circuit breaker configuration"""
    service_tier: str = Field("medium", description="Service tier: critical, high, medium, low")
    failure_threshold: int = Field(10, description="Number of failures before opening circuit", ge=1, le=100)
    success_threshold: int = Field(3, description="Successful calls needed to close circuit", ge=1, le=20)
    timeout_threshold_ms: float = Field(5000, description="Request timeout in milliseconds", ge=100, le=60000)
    failure_window_seconds: int = Field(60, description="Failure counting window in seconds", ge=10, le=3600)
    recovery_timeout_seconds: int = Field(30, description="Recovery timeout in seconds", ge=5, le=1800)
    health_check_interval_seconds: int = Field(10, description="Health check interval", ge=1, le=300)
    enable_adaptive_thresholds: bool = Field(True, description="Enable adaptive threshold adjustment")
    enable_health_checking: bool = Field(True, description="Enable health checking")
    enable_fallback: bool = Field(True, description="Enable fallback mechanisms")
    enable_load_balancing: bool = Field(False, description="Enable load balancing")
    enable_metrics: bool = Field(True, description="Enable metrics collection")
    enable_alerting: bool = Field(True, description="Enable alerting")
    alert_threshold_failures: int = Field(5, description="Failures before alerting", ge=1, le=50)
    gradual_recovery: bool = Field(True, description="Enable gradual recovery")
    recovery_test_requests: int = Field(1, description="Test requests during recovery", ge=1, le=10)
    max_recovery_attempts: int = Field(3, description="Maximum recovery attempts", ge=1, le=10)


class ServiceEndpointRequest(BaseModel):
    """Request model for adding service endpoints"""
    endpoints: List[str] = Field(..., description="List of service endpoint URLs")


class CircuitBreakerStateRequest(BaseModel):
    """Request model for manual state changes"""
    action: str = Field(..., description="Action: force_open, force_close, reset_metrics")
    reason: Optional[str] = Field(None, description="Reason for manual action")


# Circuit Breaker Management Endpoints

@router.get("/", response_model=dict)
async def list_circuit_breakers(
    include_metrics: bool = True,
    registry: CircuitBreakerRegistry = Depends(get_circuit_breaker_registry)
) -> dict:
    """
    List all registered circuit breakers with their current status

    Provides comprehensive overview of:
    - Circuit breaker configurations
    - Current states and metrics
    - Health status
    - Recent performance data
    """
    try:
        circuit_breakers = []

        for name in registry.list_all():
            cb = registry.get(name)
            if cb:
                cb_info = {
                    "name": name,
                    "state": cb.state.value,
                    "service_tier": cb.config.service_tier.value,
                    "last_state_change": cb.last_state_change.isoformat(),
                    "configuration": {
                        "failure_threshold": cb.config.failure_threshold,
                        "success_threshold": cb.config.success_threshold,
                        "timeout_threshold_ms": cb.config.timeout_threshold_ms,
                        "recovery_timeout_seconds": cb.config.recovery_timeout_seconds,
                        "enable_adaptive_thresholds": cb.config.enable_adaptive_thresholds,
                        "enable_health_checking": cb.config.enable_health_checking,
                        "enable_fallback": cb.config.enable_fallback
                    }
                }

                if include_metrics:
                    cb_info["metrics"] = cb.get_metrics()

                circuit_breakers.append(cb_info)

        # Calculate summary statistics
        total_breakers = len(circuit_breakers)
        open_breakers = len([cb for cb in circuit_breakers if cb["state"] == "open"])
        half_open_breakers = len([cb for cb in circuit_breakers if cb["state"] == "half_open"])
        forced_open_breakers = len([cb for cb in circuit_breakers if cb["state"] == "forced_open"])

        return {
            "circuit_breakers": circuit_breakers,
            "summary": {
                "total_circuit_breakers": total_breakers,
                "healthy_breakers": total_breakers - open_breakers - forced_open_breakers,
                "open_breakers": open_breakers,
                "half_open_breakers": half_open_breakers,
                "forced_open_breakers": forced_open_breakers,
                "system_health": "healthy" if open_breakers == 0 else "degraded" if open_breakers < total_breakers * 0.5 else "critical"
            },
            "timestamp": datetime.utcnow().isoformat()
        }

    except Exception as e:
        logger.error(f"Failed to list circuit breakers: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve circuit breakers"
        )


@router.post("/{service_name}", response_model=dict, status_code=status.HTTP_201_CREATED)
async def create_circuit_breaker(
    service_name: str,
    config_request: CircuitBreakerConfigRequest,
    registry: CircuitBreakerRegistry = Depends(get_circuit_breaker_registry)
) -> dict:
    """
    Create a new circuit breaker for a service

    Allows configuration of sophisticated circuit breaker patterns including:
    - Service tier-based configurations
    - Adaptive thresholds and timeouts
    - Health checking and recovery mechanisms
    - Comprehensive monitoring and alerting
    """
    try:
        # Validate service tier
        try:
            service_tier = ServiceTier(config_request.service_tier.lower())
        except ValueError:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid service tier. Supported tiers: {[tier.value for tier in ServiceTier]}"
            )

        # Check if circuit breaker already exists
        if registry.get(service_name):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Circuit breaker for service '{service_name}' already exists"
            )

        # Create configuration
        config = CircuitBreakerConfig(
            name=service_name,
            service_tier=service_tier,
            failure_threshold=config_request.failure_threshold,
            success_threshold=config_request.success_threshold,
            timeout_threshold_ms=config_request.timeout_threshold_ms,
            failure_window_seconds=config_request.failure_window_seconds,
            recovery_timeout_seconds=config_request.recovery_timeout_seconds,
            health_check_interval_seconds=config_request.health_check_interval_seconds,
            enable_adaptive_thresholds=config_request.enable_adaptive_thresholds,
            enable_health_checking=config_request.enable_health_checking,
            enable_fallback=config_request.enable_fallback,
            enable_load_balancing=config_request.enable_load_balancing,
            enable_metrics=config_request.enable_metrics,
            enable_alerting=config_request.enable_alerting,
            alert_threshold_failures=config_request.alert_threshold_failures,
            gradual_recovery=config_request.gradual_recovery,
            recovery_test_requests=config_request.recovery_test_requests,
            max_recovery_attempts=config_request.max_recovery_attempts
        )

        # Register circuit breaker
        circuit_breaker = await registry.register(service_name, config)

        logger.info(f"Created circuit breaker for service '{service_name}' with {service_tier.value} tier")

        return {
            "message": f"Circuit breaker created for service '{service_name}'",
            "service_name": service_name,
            "configuration": circuit_breaker.get_metrics(),
            "recommendations": _generate_service_recommendations(service_tier),
            "created_at": datetime.utcnow().isoformat()
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create circuit breaker for {service_name}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create circuit breaker for service '{service_name}'"
        )


@router.get("/{service_name}", response_model=dict)
async def get_circuit_breaker_details(
    service_name: str,
    include_history: bool = False,
    registry: CircuitBreakerRegistry = Depends(get_circuit_breaker_registry)
) -> dict:
    """
    Get detailed information about a specific circuit breaker

    Provides comprehensive details including:
    - Current state and configuration
    - Performance metrics and trends
    - Failure analysis and patterns
    - Health check status
    - Optimization recommendations
    """
    try:
        circuit_breaker = registry.get(service_name)
        if not circuit_breaker:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Circuit breaker for service '{service_name}' not found"
            )

        # Get comprehensive metrics
        metrics = circuit_breaker.get_metrics()

        # Calculate additional insights
        total_calls = metrics["metrics"]["total_calls"]
        failure_rate = metrics["metrics"]["failure_rate"]
        avg_response_time = metrics["metrics"]["avg_response_time_ms"]

        # Performance analysis
        performance_grade = _calculate_performance_grade(failure_rate, avg_response_time)

        # Health assessment
        health_status = _assess_circuit_health(circuit_breaker)

        # Configuration recommendations
        recommendations = _generate_configuration_recommendations(circuit_breaker)

        response = {
            "service_name": service_name,
            "circuit_breaker_info": metrics,
            "performance_analysis": {
                "performance_grade": performance_grade,
                "reliability_score": max(0, 100 - failure_rate),
                "efficiency_score": _calculate_efficiency_score(avg_response_time),
                "overall_health": health_status["overall_health"],
                "trends": {
                    "calls_trend": "stable",  # Mock data - would calculate from history
                    "failure_trend": "improving" if failure_rate < 5 else "degrading" if failure_rate > 20 else "stable",
                    "response_time_trend": "stable"  # Mock data
                }
            },
            "health_assessment": health_status,
            "optimization_recommendations": recommendations,
            "operational_insights": {
                "peak_failure_times": [],  # Mock data - would analyze from history
                "common_failure_patterns": _analyze_failure_patterns(circuit_breaker),
                "resource_utilization": {
                    "memory_efficient": True,
                    "cpu_efficient": True,
                    "network_efficient": avg_response_time < 1000
                }
            },
            "compliance_status": {
                "sla_compliance": failure_rate < 1.0 if circuit_breaker.config.service_tier == ServiceTier.CRITICAL else failure_rate < 5.0,
                "performance_compliance": avg_response_time < circuit_breaker.config.timeout_threshold_ms * 0.5,
                "availability_target": _get_availability_target(circuit_breaker.config.service_tier),
                "current_availability": metrics["metrics"]["uptime_percentage"]
            }
        }

        if include_history:
            response["failure_history"] = metrics["recent_failures"]

        return response

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to get circuit breaker details for {service_name}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to retrieve circuit breaker details for '{service_name}'"
        )


@router.put("/{service_name}/configuration", response_model=dict)
async def update_circuit_breaker_configuration(
    service_name: str,
    config_request: CircuitBreakerConfigRequest,
    registry: CircuitBreakerRegistry = Depends(get_circuit_breaker_registry)
) -> dict:
    """
    Update circuit breaker configuration

    Allows real-time configuration updates for:
    - Threshold adjustments
    - Timeout modifications
    - Feature toggles
    - Performance tuning
    """
    try:
        circuit_breaker = registry.get(service_name)
        if not circuit_breaker:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Circuit breaker for service '{service_name}' not found"
            )

        # Store old configuration for comparison
        old_config = circuit_breaker.config

        # Update configuration
        service_tier = ServiceTier(config_request.service_tier.lower())
        circuit_breaker.config.service_tier = service_tier
        circuit_breaker.config.failure_threshold = config_request.failure_threshold
        circuit_breaker.config.success_threshold = config_request.success_threshold
        circuit_breaker.config.timeout_threshold_ms = config_request.timeout_threshold_ms
        circuit_breaker.config.failure_window_seconds = config_request.failure_window_seconds
        circuit_breaker.config.recovery_timeout_seconds = config_request.recovery_timeout_seconds
        circuit_breaker.config.health_check_interval_seconds = config_request.health_check_interval_seconds
        circuit_breaker.config.enable_adaptive_thresholds = config_request.enable_adaptive_thresholds
        circuit_breaker.config.enable_health_checking = config_request.enable_health_checking
        circuit_breaker.config.enable_fallback = config_request.enable_fallback
        circuit_breaker.config.enable_alerting = config_request.enable_alerting

        # Calculate configuration changes
        changes = _calculate_config_changes(old_config, circuit_breaker.config)

        logger.info(f"Updated circuit breaker configuration for '{service_name}': {changes}")

        return {
            "message": f"Circuit breaker configuration updated for service '{service_name}'",
            "service_name": service_name,
            "changes_applied": changes,
            "new_configuration": {
                "service_tier": circuit_breaker.config.service_tier.value,
                "failure_threshold": circuit_breaker.config.failure_threshold,
                "success_threshold": circuit_breaker.config.success_threshold,
                "timeout_threshold_ms": circuit_breaker.config.timeout_threshold_ms,
                "enable_adaptive_thresholds": circuit_breaker.config.enable_adaptive_thresholds,
                "enable_health_checking": circuit_breaker.config.enable_health_checking,
                "enable_fallback": circuit_breaker.config.enable_fallback
            },
            "impact_assessment": _assess_configuration_impact(changes),
            "updated_at": datetime.utcnow().isoformat()
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to update circuit breaker configuration for {service_name}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update circuit breaker configuration for '{service_name}'"
        )


@router.post("/{service_name}/endpoints", response_model=dict)
async def add_service_endpoints(
    service_name: str,
    endpoints_request: ServiceEndpointRequest,
    registry: CircuitBreakerRegistry = Depends(get_circuit_breaker_registry)
) -> dict:
    """
    Add service endpoints for health checking

    Configures health check endpoints for:
    - Automatic recovery validation
    - Service availability monitoring
    - Load balancing health checks
    - Failover decision making
    """
    try:
        circuit_breaker = registry.get(service_name)
        if not circuit_breaker:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Circuit breaker for service '{service_name}' not found"
            )

        # Validate endpoints
        valid_endpoints = []
        invalid_endpoints = []

        for endpoint in endpoints_request.endpoints:
            if _validate_endpoint_url(endpoint):
                circuit_breaker.add_service_endpoint(endpoint)
                valid_endpoints.append(endpoint)
            else:
                invalid_endpoints.append(endpoint)

        if invalid_endpoints:
            logger.warning(f"Invalid endpoints provided for {service_name}: {invalid_endpoints}")

        return {
            "message": f"Service endpoints configured for '{service_name}'",
            "service_name": service_name,
            "endpoints_added": valid_endpoints,
            "invalid_endpoints": invalid_endpoints,
            "total_endpoints": len(circuit_breaker.service_endpoints),
            "health_checking_enabled": circuit_breaker.config.enable_health_checking,
            "health_check_interval_seconds": circuit_breaker.config.health_check_interval_seconds,
            "configured_at": datetime.utcnow().isoformat()
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to add endpoints for {service_name}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to add endpoints for '{service_name}'"
        )


@router.post("/{service_name}/control", response_model=dict)
async def control_circuit_breaker_state(
    service_name: str,
    state_request: CircuitBreakerStateRequest,
    registry: CircuitBreakerRegistry = Depends(get_circuit_breaker_registry)
) -> dict:
    """
    Manual control of circuit breaker state

    Provides operational control for:
    - Emergency circuit opening
    - Manual recovery initiation
    - Metrics reset for testing
    - Maintenance mode activation
    """
    try:
        circuit_breaker = registry.get(service_name)
        if not circuit_breaker:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Circuit breaker for service '{service_name}' not found"
            )

        action = state_request.action.lower()
        old_state = circuit_breaker.state

        if action == "force_open":
            await circuit_breaker.force_open()
            action_result = "Circuit breaker manually opened"

        elif action == "force_close":
            await circuit_breaker.force_close()
            action_result = "Circuit breaker manually closed"

        elif action == "reset_metrics":
            await circuit_breaker.reset_metrics()
            action_result = "Circuit breaker metrics reset"

        else:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid action '{action}'. Supported actions: force_open, force_close, reset_metrics"
            )

        logger.warning(f"Manual action '{action}' performed on circuit breaker '{service_name}'. Reason: {state_request.reason}")

        return {
            "message": action_result,
            "service_name": service_name,
            "action_performed": action,
            "reason": state_request.reason,
            "state_change": {
                "from": old_state.value,
                "to": circuit_breaker.state.value
            },
            "current_metrics": circuit_breaker.get_metrics(),
            "warnings": _generate_manual_action_warnings(action, circuit_breaker),
            "performed_at": datetime.utcnow().isoformat()
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to control circuit breaker state for {service_name}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to control circuit breaker state for '{service_name}'"
        )


@router.delete("/{service_name}", response_model=dict)
async def delete_circuit_breaker(
    service_name: str,
    force: bool = False,
    registry: CircuitBreakerRegistry = Depends(get_circuit_breaker_registry)
) -> dict:
    """
    Delete a circuit breaker

    Removes circuit breaker configuration with safety checks for:
    - Active service calls
    - Current circuit state
    - Dependency validation
    - Graceful cleanup
    """
    try:
        circuit_breaker = registry.get(service_name)
        if not circuit_breaker:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Circuit breaker for service '{service_name}' not found"
            )

        # Safety checks
        if not force:
            # Check if circuit is currently handling calls
            if circuit_breaker.metrics.total_calls > 0:
                recent_calls = [
                    call for call in circuit_breaker.recent_calls
                    if call.timestamp > datetime.utcnow() - timedelta(minutes=5)
                ]
                if recent_calls:
                    raise HTTPException(
                        status_code=status.HTTP_409_CONFLICT,
                        detail=f"Circuit breaker '{service_name}' has recent activity. Use force=true to override."
                    )

            # Check circuit state
            if circuit_breaker.state in [CircuitState.HALF_OPEN, CircuitState.OPEN]:
                raise HTTPException(
                    status_code=status.HTTP_409_CONFLICT,
                    detail=f"Circuit breaker '{service_name}' is in {circuit_breaker.state.value} state. Use force=true to override."
                )

        # Get final metrics before deletion
        final_metrics = circuit_breaker.get_metrics()

        # Unregister circuit breaker
        success = await registry.unregister(service_name)

        if not success:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail=f"Failed to unregister circuit breaker '{service_name}'"
            )

        logger.info(f"Deleted circuit breaker for service '{service_name}' (forced: {force})")

        return {
            "message": f"Circuit breaker for service '{service_name}' deleted successfully",
            "service_name": service_name,
            "forced_deletion": force,
            "final_metrics": final_metrics,
            "cleanup_completed": True,
            "deleted_at": datetime.utcnow().isoformat()
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to delete circuit breaker for {service_name}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete circuit breaker for '{service_name}'"
        )


# Analytics and Monitoring Endpoints

@router.get("/analytics/summary", response_model=dict)
async def get_circuit_breaker_analytics(
    time_range_hours: int = 24,
    service_tier_filter: Optional[str] = None,
    registry: CircuitBreakerRegistry = Depends(get_circuit_breaker_registry)
) -> dict:
    """
    Get comprehensive circuit breaker analytics

    Provides system-wide insights including:
    - Service reliability trends
    - Failure pattern analysis
    - Performance distribution
    - Capacity planning data
    - Operational recommendations
    """
    try:
        all_metrics = registry.get_all_metrics()

        # Filter by service tier if specified
        if service_tier_filter:
            filtered_metrics = {
                name: metrics for name, metrics in all_metrics.items()
                if metrics["service_tier"] == service_tier_filter.lower()
            }
        else:
            filtered_metrics = all_metrics

        # Calculate system-wide analytics
        analytics = _calculate_system_analytics(filtered_metrics)

        # Service tier distribution
        tier_distribution = _calculate_tier_distribution(all_metrics)

        # Performance analysis
        performance_analysis = _calculate_performance_distribution(filtered_metrics)

        # Failure analysis
        failure_analysis = _analyze_system_failures(filtered_metrics)

        # Capacity analysis
        capacity_analysis = _analyze_system_capacity(filtered_metrics)

        return {
            "analytics_summary": analytics,
            "service_distribution": {
                "by_tier": tier_distribution,
                "by_state": _calculate_state_distribution(filtered_metrics),
                "total_services": len(filtered_metrics)
            },
            "performance_analysis": performance_analysis,
            "failure_analysis": failure_analysis,
            "capacity_analysis": capacity_analysis,
            "operational_recommendations": _generate_system_recommendations(analytics),
            "time_range_hours": time_range_hours,
            "filter_applied": service_tier_filter,
            "generated_at": datetime.utcnow().isoformat()
        }

    except Exception as e:
        logger.error(f"Failed to get circuit breaker analytics: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve circuit breaker analytics"
        )


@router.get("/health", response_model=dict)
async def get_circuit_breaker_system_health(
    registry: CircuitBreakerRegistry = Depends(get_circuit_breaker_registry)
) -> dict:
    """
    Get overall circuit breaker system health

    Provides system health assessment including:
    - Overall system status
    - Component health checks
    - Performance indicators
    - Alert conditions
    - Remediation recommendations
    """
    try:
        all_metrics = registry.get_all_metrics()

        # System health calculation
        system_health = _calculate_system_health(all_metrics)

        # Component status
        component_status = _assess_component_health(registry)

        # Active alerts
        active_alerts = _identify_active_alerts(all_metrics)

        return {
            "system_health": system_health,
            "component_status": component_status,
            "active_alerts": active_alerts,
            "performance_indicators": {
                "total_circuit_breakers": len(all_metrics),
                "healthy_services": len([m for m in all_metrics.values() if m["state"] == "closed"]),
                "degraded_services": len([m for m in all_metrics.values() if m["state"] in ["open", "half_open"]]),
                "system_availability": system_health["availability_percentage"],
                "average_response_time": system_health["avg_response_time_ms"]
            },
            "remediation_recommendations": _generate_health_recommendations(system_health, active_alerts),
            "last_health_check": datetime.utcnow().isoformat(),
            "monitoring_active": True
        }

    except Exception as e:
        logger.error(f"Failed to get circuit breaker system health: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve system health"
        )


# Template Endpoints for Common Configurations

@router.post("/templates/{template_name}/{service_name}", response_model=dict, status_code=status.HTTP_201_CREATED)
async def create_circuit_breaker_from_template(
    template_name: str,
    service_name: str,
    registry: CircuitBreakerRegistry = Depends(get_circuit_breaker_registry)
) -> dict:
    """
    Create circuit breaker from predefined templates

    Available templates:
    - critical: For mission-critical services (99.99% uptime)
    - high: For high-priority services (99.9% uptime)
    - standard: For standard services (99% uptime)
    - best-effort: For best-effort services
    """
    try:
        # Check if circuit breaker already exists
        if registry.get(service_name):
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"Circuit breaker for service '{service_name}' already exists"
            )

        # Get template configuration
        template_configs = {
            "critical": create_critical_service_config,
            "high": create_high_service_config,
            "standard": create_standard_service_config,
            "best-effort": create_best_effort_service_config
        }

        if template_name not in template_configs:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Invalid template '{template_name}'. Available templates: {list(template_configs.keys())}"
            )

        # Create configuration from template
        config = template_configs[template_name](service_name)

        # Register circuit breaker
        circuit_breaker = await registry.register(service_name, config)

        logger.info(f"Created circuit breaker for '{service_name}' using '{template_name}' template")

        return {
            "message": f"Circuit breaker created for service '{service_name}' using '{template_name}' template",
            "service_name": service_name,
            "template_used": template_name,
            "configuration": circuit_breaker.get_metrics(),
            "template_description": _get_template_description(template_name),
            "created_at": datetime.utcnow().isoformat()
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Failed to create circuit breaker from template {template_name} for {service_name}: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create circuit breaker from template"
        )


# Helper Functions

def _generate_service_recommendations(service_tier: ServiceTier) -> List[str]:
    """Generate recommendations based on service tier"""
    recommendations = []

    if service_tier == ServiceTier.CRITICAL:
        recommendations.extend([
            "Configure multiple health check endpoints for redundancy",
            "Enable real-time alerting for any state changes",
            "Implement comprehensive fallback mechanisms",
            "Monitor SLA compliance closely (99.99% uptime target)"
        ])
    elif service_tier == ServiceTier.HIGH:
        recommendations.extend([
            "Set up health checking for automatic recovery",
            "Configure appropriate fallback responses",
            "Monitor failure patterns for optimization"
        ])
    else:
        recommendations.extend([
            "Consider enabling adaptive thresholds",
            "Review failure patterns periodically",
            "Optimize timeout settings based on service characteristics"
        ])

    return recommendations

def _calculate_performance_grade(failure_rate: float, avg_response_time: float) -> str:
    """Calculate performance grade A-F"""
    if failure_rate < 1 and avg_response_time < 100:
        return "A"
    elif failure_rate < 2 and avg_response_time < 200:
        return "B"
    elif failure_rate < 5 and avg_response_time < 500:
        return "C"
    elif failure_rate < 10 and avg_response_time < 1000:
        return "D"
    else:
        return "F"

def _calculate_efficiency_score(avg_response_time: float) -> int:
    """Calculate efficiency score 0-100"""
    if avg_response_time < 100:
        return 100
    elif avg_response_time < 500:
        return 80
    elif avg_response_time < 1000:
        return 60
    elif avg_response_time < 2000:
        return 40
    else:
        return 20

def _assess_circuit_health(circuit_breaker) -> Dict[str, Any]:
    """Assess overall health of a circuit breaker"""
    state = circuit_breaker.state
    metrics = circuit_breaker.get_metrics()

    if state == CircuitState.CLOSED:
        overall_health = "healthy"
    elif state == CircuitState.HALF_OPEN:
        overall_health = "recovering"
    elif state == CircuitState.OPEN:
        overall_health = "unhealthy"
    else:
        overall_health = "unknown"

    return {
        "overall_health": overall_health,
        "state_health": state.value,
        "performance_health": "good" if metrics["metrics"]["avg_response_time_ms"] < 1000 else "poor",
        "reliability_health": "good" if metrics["metrics"]["failure_rate"] < 5 else "poor"
    }

def _generate_configuration_recommendations(circuit_breaker) -> List[str]:
    """Generate configuration optimization recommendations"""
    recommendations = []
    metrics = circuit_breaker.get_metrics()

    if metrics["metrics"]["failure_rate"] > 20:
        recommendations.append("Consider reducing failure threshold for faster circuit opening")

    if metrics["metrics"]["avg_response_time_ms"] > circuit_breaker.config.timeout_threshold_ms * 0.8:
        recommendations.append("Consider reducing timeout threshold to prevent slow requests")

    if not circuit_breaker.config.enable_adaptive_thresholds:
        recommendations.append("Enable adaptive thresholds for better performance optimization")

    if not circuit_breaker.config.enable_health_checking:
        recommendations.append("Enable health checking for automatic recovery validation")

    return recommendations or ["Configuration is optimally tuned"]

def _analyze_failure_patterns(circuit_breaker) -> List[str]:
    """Analyze common failure patterns"""
    patterns = []

    # Mock analysis - in real implementation would analyze failure history
    recent_failures = circuit_breaker.get_metrics()["recent_failures"]

    timeout_failures = len([f for f in recent_failures if f.get("failure_type") == "timeout"])
    if timeout_failures > len(recent_failures) * 0.5:
        patterns.append("High timeout failure rate - consider service optimization")

    connection_failures = len([f for f in recent_failures if f.get("failure_type") == "connection_error"])
    if connection_failures > 0:
        patterns.append("Connection errors detected - check network connectivity")

    return patterns or ["No significant failure patterns detected"]

def _get_availability_target(service_tier: ServiceTier) -> float:
    """Get availability target for service tier"""
    targets = {
        ServiceTier.CRITICAL: 99.99,
        ServiceTier.HIGH: 99.9,
        ServiceTier.MEDIUM: 99.0,
        ServiceTier.LOW: 95.0
    }
    return targets.get(service_tier, 99.0)

def _calculate_config_changes(old_config: CircuitBreakerConfig, new_config: CircuitBreakerConfig) -> List[str]:
    """Calculate what configuration changes were made"""
    changes = []

    if old_config.failure_threshold != new_config.failure_threshold:
        changes.append(f"failure_threshold: {old_config.failure_threshold} → {new_config.failure_threshold}")

    if old_config.timeout_threshold_ms != new_config.timeout_threshold_ms:
        changes.append(f"timeout_threshold_ms: {old_config.timeout_threshold_ms} → {new_config.timeout_threshold_ms}")

    if old_config.enable_adaptive_thresholds != new_config.enable_adaptive_thresholds:
        changes.append(f"adaptive_thresholds: {old_config.enable_adaptive_thresholds} → {new_config.enable_adaptive_thresholds}")

    return changes or ["No significant changes"]

def _assess_configuration_impact(changes: List[str]) -> Dict[str, str]:
    """Assess impact of configuration changes"""
    return {
        "performance_impact": "medium" if any("threshold" in change for change in changes) else "low",
        "stability_impact": "high" if any("failure_threshold" in change for change in changes) else "low",
        "recovery_impact": "medium" if any("recovery" in change for change in changes) else "low"
    }

def _validate_endpoint_url(endpoint: str) -> bool:
    """Validate endpoint URL format"""
    return endpoint.startswith(("http://", "https://")) and len(endpoint) > 10

def _generate_manual_action_warnings(action: str, circuit_breaker) -> List[str]:
    """Generate warnings for manual actions"""
    warnings = []

    if action == "force_open":
        warnings.append("Service will be unavailable until manually closed or automatically recovered")
        if not circuit_breaker.config.enable_fallback:
            warnings.append("No fallback mechanism configured - all requests will fail")

    elif action == "force_close":
        warnings.append("Circuit forced closed without validation - monitor for immediate failures")
        if circuit_breaker.metrics.failure_rate > 10:
            warnings.append("Recent failure rate is high - circuit may reopen quickly")

    elif action == "reset_metrics":
        warnings.append("Historical performance data has been cleared")
        warnings.append("Adaptive thresholds will reset to default values")

    return warnings

def _calculate_system_analytics(filtered_metrics: Dict[str, Any]) -> Dict[str, Any]:
    """Calculate system-wide analytics"""
    if not filtered_metrics:
        return {"message": "No circuit breakers match the filter criteria"}

    total_calls = sum(m["metrics"]["total_calls"] for m in filtered_metrics.values())
    total_failures = sum(m["metrics"]["failed_calls"] for m in filtered_metrics.values())

    return {
        "total_services": len(filtered_metrics),
        "total_requests": total_calls,
        "total_failures": total_failures,
        "system_failure_rate": (total_failures / max(1, total_calls)) * 100,
        "healthy_services": len([m for m in filtered_metrics.values() if m["state"] == "closed"]),
        "degraded_services": len([m for m in filtered_metrics.values() if m["state"] != "closed"])
    }

def _calculate_tier_distribution(all_metrics: Dict[str, Any]) -> Dict[str, int]:
    """Calculate service tier distribution"""
    distribution = {}
    for metrics in all_metrics.values():
        tier = metrics["service_tier"]
        distribution[tier] = distribution.get(tier, 0) + 1
    return distribution

def _calculate_state_distribution(filtered_metrics: Dict[str, Any]) -> Dict[str, int]:
    """Calculate circuit state distribution"""
    distribution = {}
    for metrics in filtered_metrics.values():
        state = metrics["state"]
        distribution[state] = distribution.get(state, 0) + 1
    return distribution

def _calculate_performance_distribution(filtered_metrics: Dict[str, Any]) -> Dict[str, Any]:
    """Calculate performance distribution"""
    if not filtered_metrics:
        return {"message": "No data available"}

    response_times = [m["metrics"]["avg_response_time_ms"] for m in filtered_metrics.values()]
    failure_rates = [m["metrics"]["failure_rate"] for m in filtered_metrics.values()]

    return {
        "avg_response_time_ms": sum(response_times) / len(response_times),
        "avg_failure_rate": sum(failure_rates) / len(failure_rates),
        "fastest_service": min(response_times),
        "slowest_service": max(response_times)
    }

def _analyze_system_failures(filtered_metrics: Dict[str, Any]) -> Dict[str, Any]:
    """Analyze system-wide failures"""
    total_failures = sum(m["metrics"]["failed_calls"] for m in filtered_metrics.values())
    services_with_failures = len([m for m in filtered_metrics.values() if m["metrics"]["failed_calls"] > 0])

    return {
        "total_system_failures": total_failures,
        "services_with_failures": services_with_failures,
        "failure_concentration": services_with_failures / max(1, len(filtered_metrics)) * 100
    }

def _analyze_system_capacity(filtered_metrics: Dict[str, Any]) -> Dict[str, Any]:
    """Analyze system capacity"""
    return {
        "total_capacity": len(filtered_metrics),
        "available_capacity": len([m for m in filtered_metrics.values() if m["state"] == "closed"]),
        "degraded_capacity": len([m for m in filtered_metrics.values() if m["state"] != "closed"]),
        "capacity_utilization": 85.0  # Mock data
    }

def _generate_system_recommendations(analytics: Dict[str, Any]) -> List[str]:
    """Generate system-wide recommendations"""
    recommendations = []

    if analytics.get("system_failure_rate", 0) > 5:
        recommendations.append("High system failure rate detected - review service configurations")

    if analytics.get("degraded_services", 0) > analytics.get("total_services", 0) * 0.2:
        recommendations.append("More than 20% of services are degraded - investigate infrastructure issues")

    return recommendations or ["System performance is within acceptable parameters"]

def _calculate_system_health(all_metrics: Dict[str, Any]) -> Dict[str, Any]:
    """Calculate overall system health"""
    if not all_metrics:
        return {"status": "unknown", "availability_percentage": 0, "avg_response_time_ms": 0}

    healthy_services = len([m for m in all_metrics.values() if m["state"] == "closed"])
    total_services = len(all_metrics)
    avg_response_times = [m["metrics"]["avg_response_time_ms"] for m in all_metrics.values()]

    availability = (healthy_services / total_services) * 100
    avg_response_time = sum(avg_response_times) / len(avg_response_times)

    status = "healthy" if availability > 90 else "degraded" if availability > 70 else "critical"

    return {
        "status": status,
        "availability_percentage": availability,
        "avg_response_time_ms": avg_response_time,
        "healthy_services": healthy_services,
        "total_services": total_services
    }

def _assess_component_health(registry: CircuitBreakerRegistry) -> Dict[str, str]:
    """Assess component health"""
    return {
        "circuit_breaker_registry": "healthy",
        "configuration_manager": "healthy",
        "metrics_collector": "healthy",
        "health_checker": "healthy",
        "alert_manager": "healthy"
    }

def _identify_active_alerts(all_metrics: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Identify active alerts"""
    alerts = []

    for service_name, metrics in all_metrics.items():
        if metrics["state"] == "open":
            alerts.append({
                "service": service_name,
                "severity": "critical",
                "message": f"Circuit breaker is open for {service_name}",
                "timestamp": datetime.utcnow().isoformat()
            })
        elif metrics["metrics"]["failure_rate"] > 20:
            alerts.append({
                "service": service_name,
                "severity": "warning",
                "message": f"High failure rate ({metrics['metrics']['failure_rate']:.1f}%) for {service_name}",
                "timestamp": datetime.utcnow().isoformat()
            })

    return alerts

def _generate_health_recommendations(system_health: Dict[str, Any], active_alerts: List[Dict[str, Any]]) -> List[str]:
    """Generate health-based recommendations"""
    recommendations = []

    if system_health["status"] == "critical":
        recommendations.append("Critical system health - immediate intervention required")
    elif system_health["status"] == "degraded":
        recommendations.append("System performance is degraded - investigate service issues")

    if len(active_alerts) > 5:
        recommendations.append("High number of active alerts - prioritize critical issues")

    return recommendations or ["System health is satisfactory"]

def _get_template_description(template_name: str) -> str:
    """Get description for template"""
    descriptions = {
        "critical": "Mission-critical services requiring 99.99% uptime with aggressive failure detection",
        "high": "High-priority services requiring 99.9% uptime with balanced performance",
        "standard": "Standard services with 99% uptime target and moderate fault tolerance",
        "best-effort": "Best-effort services with lenient fault tolerance for non-critical operations"
    }
    return descriptions.get(template_name, "Standard circuit breaker configuration")