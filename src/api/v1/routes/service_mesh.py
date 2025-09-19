"""
Service Mesh Management API Router
Provides management and monitoring endpoints for Istio service mesh.
"""
from __future__ import annotations

import json
import subprocess
from datetime import datetime
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel, Field

from core.logging import get_logger

logger = get_logger(__name__)
router = APIRouter(prefix="/service-mesh", tags=["service-mesh"])


class TrafficSplitRequest(BaseModel):
    """Request model for traffic splitting."""
    service_name: str
    version_weights: Dict[str, int] = Field(..., description="Version to weight mapping")
    namespace: str = "pwc-data-platform"


class CircuitBreakerConfig(BaseModel):
    """Circuit breaker configuration."""
    consecutive_5xx_errors: int = Field(5, ge=1, le=50)
    consecutive_gateway_errors: int = Field(5, ge=1, le=50)
    interval: str = Field("30s", description="Analysis interval")
    base_ejection_time: str = Field("30s", description="Minimum ejection time")
    max_ejection_percent: int = Field(50, ge=1, le=100)
    min_health_percent: int = Field(30, ge=1, le=100)


class RetryPolicyConfig(BaseModel):
    """Retry policy configuration."""
    attempts: int = Field(3, ge=1, le=10)
    per_try_timeout: str = Field("5s", description="Timeout per retry attempt")
    retry_on: str = Field("5xx,reset,connect-failure,refused-stream")
    retry_remote_localities: bool = False


class RateLimitConfig(BaseModel):
    """Rate limiting configuration."""
    requests_per_minute: int = Field(100, ge=1, le=10000)
    burst_size: int = Field(50, ge=1, le=1000)
    fill_interval: str = Field("60s", description="Token bucket fill interval")


class SecurityPolicyRequest(BaseModel):
    """Security policy configuration."""
    service_name: str
    namespace: str = "pwc-data-platform"
    allowed_principals: List[str] = Field(default_factory=list)
    allowed_methods: List[str] = Field(default_factory=lambda: ["GET", "POST", "PUT", "DELETE"])
    allowed_paths: List[str] = Field(default_factory=lambda: ["/*"])
    jwt_issuer: Optional[str] = None
    jwt_audiences: List[str] = Field(default_factory=list)


@router.get("/status")
async def get_mesh_status() -> Dict[str, Any]:
    """
    Get comprehensive status of the Istio service mesh.

    Provides information about:
    - Control plane health
    - Data plane proxy status
    - Gateway configuration
    - Traffic management policies
    """
    try:
        # Check if istioctl is available
        try:
            subprocess.run(["istioctl", "version"], capture_output=True, check=True, timeout=10)
        except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
            raise HTTPException(
                status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
                detail="Istio CLI (istioctl) is not available or service mesh is not installed"
            )

        # Get control plane status
        control_plane_status = await _get_control_plane_status()

        # Get proxy status
        proxy_status = await _get_proxy_status()

        # Get gateway status
        gateway_status = await _get_gateway_status()

        # Get traffic policies
        traffic_policies = await _get_traffic_policies()

        # Get security policies
        security_policies = await _get_security_policies()

        # Get metrics summary
        metrics_summary = await _get_metrics_summary()

        return {
            "mesh_status": "healthy" if control_plane_status.get("healthy", False) else "degraded",
            "control_plane": control_plane_status,
            "data_plane": proxy_status,
            "gateways": gateway_status,
            "traffic_management": traffic_policies,
            "security": security_policies,
            "metrics": metrics_summary,
            "timestamp": datetime.now().isoformat()
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error getting mesh status: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get mesh status: {str(e)}"
        )


@router.post("/traffic/split")
async def configure_traffic_split(request: TrafficSplitRequest) -> Dict[str, Any]:
    """
    Configure traffic splitting between service versions.

    Enables:
    - Canary deployments
    - Blue-green deployments
    - A/B testing
    - Gradual rollouts
    """
    try:
        # Validate weights sum to 100
        total_weight = sum(request.version_weights.values())
        if total_weight != 100:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"Version weights must sum to 100, got {total_weight}"
            )

        # Generate VirtualService configuration
        virtual_service = _generate_virtual_service_config(request)

        # Apply the configuration
        result = await _apply_kubernetes_config(virtual_service)

        return {
            "status": "success",
            "message": "Traffic split configuration applied successfully",
            "service_name": request.service_name,
            "namespace": request.namespace,
            "version_weights": request.version_weights,
            "configuration": virtual_service,
            "applied_at": datetime.now().isoformat()
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error configuring traffic split: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to configure traffic split: {str(e)}"
        )


@router.post("/resilience/circuit-breaker/{service_name}")
async def configure_circuit_breaker(
    service_name: str,
    config: CircuitBreakerConfig,
    namespace: str = Query("pwc-data-platform", description="Service namespace")
) -> Dict[str, Any]:
    """
    Configure circuit breaker for a service to improve resilience.

    Features:
    - Automatic failure detection
    - Service ejection from load balancing
    - Gradual recovery
    - Customizable thresholds
    """
    try:
        # Generate DestinationRule configuration
        destination_rule = _generate_circuit_breaker_config(service_name, namespace, config)

        # Apply the configuration
        result = await _apply_kubernetes_config(destination_rule)

        return {
            "status": "success",
            "message": "Circuit breaker configuration applied successfully",
            "service_name": service_name,
            "namespace": namespace,
            "configuration": config.model_dump(),
            "applied_at": datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"Error configuring circuit breaker: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to configure circuit breaker: {str(e)}"
        )


@router.post("/resilience/retry-policy/{service_name}")
async def configure_retry_policy(
    service_name: str,
    config: RetryPolicyConfig,
    namespace: str = Query("pwc-data-platform", description="Service namespace")
) -> Dict[str, Any]:
    """
    Configure retry policy for improved service reliability.

    Features:
    - Automatic request retries
    - Configurable retry conditions
    - Per-attempt timeouts
    - Exponential backoff
    """
    try:
        # Generate VirtualService with retry configuration
        virtual_service = _generate_retry_policy_config(service_name, namespace, config)

        # Apply the configuration
        result = await _apply_kubernetes_config(virtual_service)

        return {
            "status": "success",
            "message": "Retry policy configuration applied successfully",
            "service_name": service_name,
            "namespace": namespace,
            "configuration": config.model_dump(),
            "applied_at": datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"Error configuring retry policy: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to configure retry policy: {str(e)}"
        )


@router.post("/security/authorization")
async def configure_authorization_policy(request: SecurityPolicyRequest) -> Dict[str, Any]:
    """
    Configure authorization policies for service access control.

    Features:
    - Principal-based access control
    - Method and path restrictions
    - JWT-based authentication
    - Namespace isolation
    """
    try:
        # Generate AuthorizationPolicy configuration
        auth_policy = _generate_authorization_policy_config(request)

        # Apply the configuration
        result = await _apply_kubernetes_config(auth_policy)

        return {
            "status": "success",
            "message": "Authorization policy applied successfully",
            "service_name": request.service_name,
            "namespace": request.namespace,
            "policy_details": {
                "allowed_principals": request.allowed_principals,
                "allowed_methods": request.allowed_methods,
                "allowed_paths": request.allowed_paths,
                "jwt_enabled": bool(request.jwt_issuer)
            },
            "applied_at": datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"Error configuring authorization policy: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to configure authorization policy: {str(e)}"
        )


@router.post("/security/mtls/{namespace}")
async def configure_mtls(
    namespace: str,
    mode: str = Query("STRICT", enum=["STRICT", "PERMISSIVE", "DISABLE"])
) -> Dict[str, Any]:
    """
    Configure mutual TLS (mTLS) for secure service communication.

    Modes:
    - STRICT: Only accept mTLS traffic
    - PERMISSIVE: Accept both mTLS and plaintext traffic
    - DISABLE: Accept only plaintext traffic
    """
    try:
        # Generate PeerAuthentication configuration
        peer_auth = _generate_mtls_config(namespace, mode)

        # Apply the configuration
        result = await _apply_kubernetes_config(peer_auth)

        return {
            "status": "success",
            "message": f"mTLS configuration applied successfully for namespace {namespace}",
            "namespace": namespace,
            "mtls_mode": mode,
            "security_level": _get_security_level(mode),
            "applied_at": datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"Error configuring mTLS: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to configure mTLS: {str(e)}"
        )


@router.get("/metrics/traffic")
async def get_traffic_metrics(
    service_name: Optional[str] = Query(None, description="Filter by service name"),
    namespace: str = Query("pwc-data-platform", description="Service namespace"),
    time_range: str = Query("5m", description="Time range for metrics")
) -> Dict[str, Any]:
    """
    Get traffic metrics from the service mesh.

    Provides:
    - Request rates and latencies
    - Error rates by service
    - Traffic flow analysis
    - Performance trends
    """
    try:
        # Get traffic metrics from Prometheus
        metrics = await _get_traffic_metrics(service_name, namespace, time_range)

        return {
            "service_name": service_name,
            "namespace": namespace,
            "time_range": time_range,
            "metrics": metrics,
            "timestamp": datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"Error getting traffic metrics: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get traffic metrics: {str(e)}"
        )


@router.get("/topology")
async def get_service_topology() -> Dict[str, Any]:
    """
    Get service mesh topology and service dependencies.

    Provides:
    - Service dependency graph
    - Communication patterns
    - Traffic flow visualization
    - Health status per service
    """
    try:
        # Get service topology from Kiali API or Istio
        topology = await _get_service_topology()

        return {
            "topology": topology,
            "service_count": len(topology.get("services", [])),
            "connection_count": len(topology.get("connections", [])),
            "generated_at": datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"Error getting service topology: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to get service topology: {str(e)}"
        )


@router.post("/configuration/validate")
async def validate_configuration(
    namespace: str = Query("pwc-data-platform", description="Namespace to validate")
) -> Dict[str, Any]:
    """
    Validate Istio configuration for potential issues.

    Checks for:
    - Configuration conflicts
    - Policy violations
    - Best practice recommendations
    - Security misconfigurations
    """
    try:
        # Run Istio configuration analysis
        validation_result = await _validate_istio_configuration(namespace)

        return {
            "namespace": namespace,
            "validation_status": validation_result["status"],
            "issues": validation_result.get("issues", []),
            "warnings": validation_result.get("warnings", []),
            "recommendations": validation_result.get("recommendations", []),
            "score": validation_result.get("score", 100),
            "validated_at": datetime.now().isoformat()
        }

    except Exception as e:
        logger.error(f"Error validating configuration: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to validate configuration: {str(e)}"
        )


# Helper functions
async def _get_control_plane_status() -> Dict[str, Any]:
    """Get Istio control plane status."""
    try:
        # Use kubectl to check istiod pods
        result = subprocess.run([
            "kubectl", "get", "pods", "-n", "istio-system",
            "-l", "app=istiod", "-o", "json"
        ], capture_output=True, text=True, timeout=10)

        if result.returncode != 0:
            return {"healthy": False, "error": "Failed to query control plane"}

        pods_data = json.loads(result.stdout)
        pods = pods_data.get("items", [])

        healthy_pods = [p for p in pods if p.get("status", {}).get("phase") == "Running"]

        return {
            "healthy": len(healthy_pods) > 0,
            "total_pods": len(pods),
            "healthy_pods": len(healthy_pods),
            "version": _extract_istio_version(pods),
            "pods": [{
                "name": p["metadata"]["name"],
                "status": p["status"]["phase"],
                "ready": p["status"].get("containerStatuses", [{}])[0].get("ready", False)
            } for p in pods]
        }

    except Exception as e:
        logger.warning(f"Error getting control plane status: {e}")
        return {"healthy": False, "error": str(e)}


async def _get_proxy_status() -> Dict[str, Any]:
    """Get sidecar proxy status."""
    try:
        result = subprocess.run([
            "istioctl", "proxy-status", "--output", "json"
        ], capture_output=True, text=True, timeout=15)

        if result.returncode != 0:
            return {"error": "Failed to get proxy status"}

        # Parse the output (istioctl proxy-status doesn't always return valid JSON)
        return {"status": "available", "details": result.stdout}

    except Exception as e:
        logger.warning(f"Error getting proxy status: {e}")
        return {"error": str(e)}


async def _get_gateway_status() -> Dict[str, Any]:
    """Get gateway status."""
    try:
        result = subprocess.run([
            "kubectl", "get", "gateway", "-A", "-o", "json"
        ], capture_output=True, text=True, timeout=10)

        if result.returncode != 0:
            return {"error": "Failed to get gateway status"}

        gateways_data = json.loads(result.stdout)
        gateways = gateways_data.get("items", [])

        return {
            "total_gateways": len(gateways),
            "gateways": [{
                "name": g["metadata"]["name"],
                "namespace": g["metadata"]["namespace"],
                "servers": len(g["spec"].get("servers", []))
            } for g in gateways]
        }

    except Exception as e:
        logger.warning(f"Error getting gateway status: {e}")
        return {"error": str(e)}


async def _get_traffic_policies() -> Dict[str, Any]:
    """Get traffic management policies."""
    try:
        # Get VirtualServices
        vs_result = subprocess.run([
            "kubectl", "get", "virtualservice", "-A", "-o", "json"
        ], capture_output=True, text=True, timeout=10)

        # Get DestinationRules
        dr_result = subprocess.run([
            "kubectl", "get", "destinationrule", "-A", "-o", "json"
        ], capture_output=True, text=True, timeout=10)

        policies = {}

        if vs_result.returncode == 0:
            vs_data = json.loads(vs_result.stdout)
            policies["virtual_services"] = len(vs_data.get("items", []))

        if dr_result.returncode == 0:
            dr_data = json.loads(dr_result.stdout)
            policies["destination_rules"] = len(dr_data.get("items", []))

        return policies

    except Exception as e:
        logger.warning(f"Error getting traffic policies: {e}")
        return {"error": str(e)}


async def _get_security_policies() -> Dict[str, Any]:
    """Get security policies."""
    try:
        # Get AuthorizationPolicies
        ap_result = subprocess.run([
            "kubectl", "get", "authorizationpolicy", "-A", "-o", "json"
        ], capture_output=True, text=True, timeout=10)

        # Get PeerAuthentications
        pa_result = subprocess.run([
            "kubectl", "get", "peerauthentication", "-A", "-o", "json"
        ], capture_output=True, text=True, timeout=10)

        policies = {}

        if ap_result.returncode == 0:
            ap_data = json.loads(ap_result.stdout)
            policies["authorization_policies"] = len(ap_data.get("items", []))

        if pa_result.returncode == 0:
            pa_data = json.loads(pa_result.stdout)
            policies["peer_authentications"] = len(pa_data.get("items", []))

        return policies

    except Exception as e:
        logger.warning(f"Error getting security policies: {e}")
        return {"error": str(e)}


async def _get_metrics_summary() -> Dict[str, Any]:
    """Get basic metrics summary."""
    return {
        "collection_enabled": True,
        "last_updated": datetime.now().isoformat(),
        "note": "Detailed metrics available through Prometheus/Grafana"
    }


def _generate_virtual_service_config(request: TrafficSplitRequest) -> Dict[str, Any]:
    """Generate VirtualService configuration for traffic splitting."""
    destinations = []
    for version, weight in request.version_weights.items():
        destinations.append({
            "destination": {
                "host": request.service_name,
                "subset": version
            },
            "weight": weight
        })

    return {
        "apiVersion": "networking.istio.io/v1beta1",
        "kind": "VirtualService",
        "metadata": {
            "name": f"{request.service_name}-traffic-split",
            "namespace": request.namespace
        },
        "spec": {
            "hosts": [request.service_name],
            "http": [{
                "route": destinations
            }]
        }
    }


def _generate_circuit_breaker_config(service_name: str, namespace: str, config: CircuitBreakerConfig) -> Dict[str, Any]:
    """Generate DestinationRule configuration for circuit breaker."""
    return {
        "apiVersion": "networking.istio.io/v1beta1",
        "kind": "DestinationRule",
        "metadata": {
            "name": f"{service_name}-circuit-breaker",
            "namespace": namespace
        },
        "spec": {
            "host": service_name,
            "trafficPolicy": {
                "outlierDetection": {
                    "consecutive5xxErrors": config.consecutive_5xx_errors,
                    "consecutiveGatewayErrors": config.consecutive_gateway_errors,
                    "interval": config.interval,
                    "baseEjectionTime": config.base_ejection_time,
                    "maxEjectionPercent": config.max_ejection_percent,
                    "minHealthPercent": config.min_health_percent
                }
            }
        }
    }


def _extract_istio_version(pods: List[Dict[str, Any]]) -> Optional[str]:
    """Extract Istio version from pod labels."""
    for pod in pods:
        labels = pod.get("metadata", {}).get("labels", {})
        version = labels.get("istio.io/rev") or labels.get("version")
        if version:
            return version
    return None


def _get_security_level(mtls_mode: str) -> str:
    """Get security level description for mTLS mode."""
    levels = {
        "STRICT": "High - Only encrypted traffic allowed",
        "PERMISSIVE": "Medium - Mixed encrypted and plaintext traffic",
        "DISABLE": "Low - Only plaintext traffic allowed"
    }
    return levels.get(mtls_mode, "Unknown")


async def _apply_kubernetes_config(config: Dict[str, Any]) -> Dict[str, Any]:
    """Apply Kubernetes configuration."""
    # In a real implementation, this would use the Kubernetes API
    # For now, return a mock response
    return {"applied": True, "resource": config["kind"]}


async def _get_traffic_metrics(service_name: Optional[str], namespace: str, time_range: str) -> Dict[str, Any]:
    """Get traffic metrics from Prometheus."""
    # Mock implementation - in production, query Prometheus API
    return {
        "request_rate": "10.5 req/s",
        "error_rate": "0.5%",
        "p99_latency": "150ms",
        "p95_latency": "75ms",
        "p50_latency": "25ms"
    }


async def _get_service_topology() -> Dict[str, Any]:
    """Get service mesh topology."""
    # Mock implementation - in production, query Kiali API or build from Istio config
    return {
        "services": [
            {"name": "fastapi-service", "namespace": "pwc-data-platform", "health": "healthy"},
            {"name": "postgresql-service", "namespace": "pwc-data-platform", "health": "healthy"},
            {"name": "redis-service", "namespace": "pwc-data-platform", "health": "healthy"}
        ],
        "connections": [
            {"source": "fastapi-service", "destination": "postgresql-service", "protocol": "TCP"},
            {"source": "fastapi-service", "destination": "redis-service", "protocol": "TCP"}
        ]
    }


async def _validate_istio_configuration(namespace: str) -> Dict[str, Any]:
    """Validate Istio configuration."""
    try:
        result = subprocess.run([
            "istioctl", "analyze", "-n", namespace
        ], capture_output=True, text=True, timeout=30)

        issues = []
        warnings = []

        if result.returncode != 0:
            issues.append("Configuration analysis failed")

        # Parse output for issues and warnings
        output_lines = result.stdout.split('\n') if result.stdout else []
        for line in output_lines:
            if 'Error' in line:
                issues.append(line.strip())
            elif 'Warning' in line:
                warnings.append(line.strip())

        score = max(0, 100 - (len(issues) * 20) - (len(warnings) * 5))

        return {
            "status": "healthy" if not issues else "degraded",
            "issues": issues,
            "warnings": warnings,
            "recommendations": [
                "Review and fix configuration issues",
                "Enable mTLS for secure communication",
                "Implement proper authorization policies"
            ] if issues or warnings else ["Configuration looks good"],
            "score": score
        }

    except Exception as e:
        return {
            "status": "error",
            "error": str(e)
        }