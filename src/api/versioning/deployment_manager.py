"""
Enterprise API Deployment Manager
=================================

Zero-downtime deployment management with traffic routing, health monitoring,
and automatic rollback capabilities for API versioning.
"""
from __future__ import annotations

import asyncio
import json
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Callable, Set
import uuid

from fastapi import FastAPI, Request, Response, HTTPException
from fastapi.middleware.base import BaseHTTPMiddleware
from pydantic import BaseModel, Field

from core.logging import get_logger

logger = get_logger(__name__)


class DeploymentState(Enum):
    """Deployment states."""
    INACTIVE = "inactive"
    DEPLOYING = "deploying"
    ACTIVE = "active"
    DRAINING = "draining"
    FAILED = "failed"
    ROLLED_BACK = "rolled_back"


class HealthStatus(Enum):
    """Health check status."""
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"


class TrafficRoutingStrategy(Enum):
    """Traffic routing strategies."""
    WEIGHTED = "weighted"
    HEADER_BASED = "header_based"
    GEOGRAPHIC = "geographic"
    USER_BASED = "user_based"
    CANARY = "canary"


@dataclass
class HealthMetrics:
    """Health monitoring metrics."""
    status: HealthStatus = HealthStatus.UNKNOWN
    response_time_ms: float = 0.0
    error_rate: float = 0.0
    cpu_usage: float = 0.0
    memory_usage: float = 0.0
    request_count: int = 0
    success_count: int = 0
    error_count: int = 0
    last_check: datetime = field(default_factory=datetime.utcnow)
    uptime_seconds: float = 0.0


@dataclass
class DeploymentVersion:
    """API version deployment information."""
    version: str
    deployment_id: str
    state: DeploymentState = DeploymentState.INACTIVE
    health: HealthMetrics = field(default_factory=HealthMetrics)
    traffic_weight: float = 0.0
    deployed_at: datetime = field(default_factory=datetime.utcnow)
    instances: List[str] = field(default_factory=list)
    config: Dict[str, Any] = field(default_factory=dict)
    rollback_version: Optional[str] = None


class HealthChecker:
    """Advanced health checking for API deployments."""

    def __init__(self, check_interval: int = 30):
        self.check_interval = check_interval
        self.health_history: Dict[str, List[HealthMetrics]] = {}
        self.running = False
        self.check_tasks: Set[asyncio.Task] = set()

    async def start_monitoring(self, deployments: Dict[str, DeploymentVersion]):
        """Start health monitoring for all deployments."""
        self.running = True
        logger.info("Starting health monitoring")

        for deployment_id, deployment in deployments.items():
            task = asyncio.create_task(self._monitor_deployment(deployment))
            self.check_tasks.add(task)

    async def stop_monitoring(self):
        """Stop all health monitoring."""
        self.running = False
        logger.info("Stopping health monitoring")

        for task in self.check_tasks:
            task.cancel()

        await asyncio.gather(*self.check_tasks, return_exceptions=True)
        self.check_tasks.clear()

    async def _monitor_deployment(self, deployment: DeploymentVersion):
        """Monitor health of a specific deployment."""
        while self.running and deployment.state in [DeploymentState.ACTIVE, DeploymentState.DEPLOYING]:
            try:
                # Perform health checks
                health_metrics = await self._perform_health_check(deployment)
                deployment.health = health_metrics

                # Store health history
                if deployment.deployment_id not in self.health_history:
                    self.health_history[deployment.deployment_id] = []

                self.health_history[deployment.deployment_id].append(health_metrics)

                # Keep only last 100 health checks
                if len(self.health_history[deployment.deployment_id]) > 100:
                    self.health_history[deployment.deployment_id] = \
                        self.health_history[deployment.deployment_id][-100:]

                # Check for health degradation
                await self._evaluate_health_trends(deployment)

                await asyncio.sleep(self.check_interval)

            except Exception as e:
                logger.error(f"Health check failed for deployment {deployment.deployment_id}: {e}")
                deployment.health.status = HealthStatus.UNKNOWN
                await asyncio.sleep(self.check_interval)

    async def _perform_health_check(self, deployment: DeploymentVersion) -> HealthMetrics:
        """Perform comprehensive health check on deployment."""
        start_time = time.time()

        try:
            # Simulate health check calls to instances
            # In production, this would make actual HTTP requests
            metrics = HealthMetrics()

            # Check each instance
            total_response_time = 0
            healthy_instances = 0
            error_count = 0

            for instance in deployment.instances:
                try:
                    # Simulate instance health check
                    instance_response_time = await self._check_instance_health(instance)
                    total_response_time += instance_response_time
                    healthy_instances += 1

                except Exception:
                    error_count += 1

            # Calculate metrics
            if healthy_instances > 0:
                metrics.response_time_ms = total_response_time / healthy_instances
                metrics.error_rate = error_count / len(deployment.instances)

                # Determine overall health status
                if error_count == 0:
                    metrics.status = HealthStatus.HEALTHY
                elif error_count < len(deployment.instances) * 0.3:
                    metrics.status = HealthStatus.DEGRADED
                else:
                    metrics.status = HealthStatus.UNHEALTHY
            else:
                metrics.status = HealthStatus.UNHEALTHY
                metrics.error_rate = 1.0

            # Update other metrics
            metrics.request_count = deployment.health.request_count
            metrics.success_count = deployment.health.success_count
            metrics.error_count = deployment.health.error_count
            metrics.uptime_seconds = (datetime.utcnow() - deployment.deployed_at).total_seconds()
            metrics.last_check = datetime.utcnow()

            # Simulate resource usage
            metrics.cpu_usage = min(80.0, metrics.response_time_ms / 10)
            metrics.memory_usage = min(90.0, metrics.request_count / 100)

            return metrics

        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return HealthMetrics(status=HealthStatus.UNKNOWN)

    async def _check_instance_health(self, instance: str) -> float:
        """Check health of individual instance."""
        # Simulate network call to instance health endpoint
        await asyncio.sleep(0.01)  # Simulate network latency

        # Simulate response time (50-200ms)
        import random
        response_time = random.uniform(50, 200)

        # Simulate occasional failures
        if random.random() < 0.05:  # 5% failure rate
            raise Exception("Instance health check failed")

        return response_time

    async def _evaluate_health_trends(self, deployment: DeploymentVersion):
        """Evaluate health trends to detect issues early."""
        history = self.health_history.get(deployment.deployment_id, [])

        if len(history) < 5:
            return

        # Check recent health trend
        recent_checks = history[-5:]
        degraded_count = sum(1 for check in recent_checks
                           if check.status in [HealthStatus.DEGRADED, HealthStatus.UNHEALTHY])

        # Alert if degradation trend detected
        if degraded_count >= 3:
            logger.warning(f"Health degradation detected for deployment {deployment.deployment_id}")

        # Check response time trend
        response_times = [check.response_time_ms for check in recent_checks]
        if len(response_times) >= 3:
            avg_response_time = sum(response_times) / len(response_times)
            if avg_response_time > 1000:  # More than 1 second
                logger.warning(f"High response time detected for deployment {deployment.deployment_id}: {avg_response_time:.2f}ms")


class TrafficRouter:
    """Advanced traffic routing for API deployments."""

    def __init__(self):
        self.routing_rules: List[Dict[str, Any]] = []
        self.active_deployments: Dict[str, DeploymentVersion] = {}
        self.request_metrics: Dict[str, int] = {}

    def register_deployment(self, deployment: DeploymentVersion):
        """Register a deployment for traffic routing."""
        self.active_deployments[deployment.deployment_id] = deployment
        logger.info(f"Registered deployment {deployment.deployment_id} for traffic routing")

    def unregister_deployment(self, deployment_id: str):
        """Unregister a deployment from traffic routing."""
        if deployment_id in self.active_deployments:
            del self.active_deployments[deployment_id]
            logger.info(f"Unregistered deployment {deployment_id} from traffic routing")

    def set_traffic_weights(self, weights: Dict[str, float]):
        """Set traffic weights for deployments."""
        total_weight = sum(weights.values())
        if abs(total_weight - 100.0) > 0.01:
            raise ValueError(f"Traffic weights must sum to 100, got {total_weight}")

        for deployment_id, weight in weights.items():
            if deployment_id in self.active_deployments:
                self.active_deployments[deployment_id].traffic_weight = weight

        logger.info(f"Updated traffic weights: {weights}")

    async def route_request(self, request: Request) -> str:
        """Route request to appropriate deployment based on routing strategy."""
        # Extract routing information from request
        routing_info = self._extract_routing_info(request)

        # Apply routing rules
        for rule in self.routing_rules:
            if self._matches_rule(routing_info, rule):
                target_deployment = rule["target_deployment"]
                if target_deployment in self.active_deployments:
                    deployment = self.active_deployments[target_deployment]
                    if deployment.state == DeploymentState.ACTIVE:
                        self._record_request(target_deployment)
                        return target_deployment

        # Fallback to weighted routing
        return await self._weighted_routing()

    def _extract_routing_info(self, request: Request) -> Dict[str, Any]:
        """Extract routing information from request."""
        return {
            "path": str(request.url.path),
            "method": request.method,
            "headers": dict(request.headers),
            "query_params": dict(request.query_params),
            "client_ip": request.client.host if request.client else None,
            "user_agent": request.headers.get("user-agent", ""),
            "api_version": request.headers.get("api-version", ""),
            "user_id": request.headers.get("x-user-id", "")
        }

    def _matches_rule(self, routing_info: Dict[str, Any], rule: Dict[str, Any]) -> bool:
        """Check if request matches routing rule."""
        conditions = rule.get("conditions", {})

        for field, expected_value in conditions.items():
            if field not in routing_info:
                return False

            actual_value = routing_info[field]
            if isinstance(expected_value, str):
                if expected_value not in actual_value:
                    return False
            elif isinstance(expected_value, list):
                if actual_value not in expected_value:
                    return False
            elif actual_value != expected_value:
                return False

        return True

    async def _weighted_routing(self) -> str:
        """Route request based on traffic weights."""
        active_deployments = {
            deployment_id: deployment
            for deployment_id, deployment in self.active_deployments.items()
            if deployment.state == DeploymentState.ACTIVE and deployment.traffic_weight > 0
        }

        if not active_deployments:
            raise HTTPException(status_code=503, detail="No active deployments available")

        # Generate random number for weighted selection
        import random
        rand = random.uniform(0, 100)

        cumulative_weight = 0
        for deployment_id, deployment in active_deployments.items():
            cumulative_weight += deployment.traffic_weight
            if rand <= cumulative_weight:
                self._record_request(deployment_id)
                return deployment_id

        # Fallback to first active deployment
        deployment_id = next(iter(active_deployments))
        self._record_request(deployment_id)
        return deployment_id

    def _record_request(self, deployment_id: str):
        """Record request metrics for deployment."""
        if deployment_id not in self.request_metrics:
            self.request_metrics[deployment_id] = 0
        self.request_metrics[deployment_id] += 1

        # Update deployment metrics
        if deployment_id in self.active_deployments:
            deployment = self.active_deployments[deployment_id]
            deployment.health.request_count += 1

    def add_routing_rule(self, rule: Dict[str, Any]):
        """Add custom routing rule."""
        self.routing_rules.append(rule)
        logger.info(f"Added routing rule: {rule}")

    def remove_routing_rule(self, rule_id: str):
        """Remove routing rule by ID."""
        self.routing_rules = [rule for rule in self.routing_rules if rule.get("id") != rule_id]
        logger.info(f"Removed routing rule: {rule_id}")

    def get_traffic_distribution(self) -> Dict[str, Dict[str, Any]]:
        """Get current traffic distribution across deployments."""
        distribution = {}

        for deployment_id, deployment in self.active_deployments.items():
            distribution[deployment_id] = {
                "version": deployment.version,
                "traffic_weight": deployment.traffic_weight,
                "request_count": self.request_metrics.get(deployment_id, 0),
                "state": deployment.state.value,
                "health_status": deployment.health.status.value
            }

        return distribution


class DeploymentManager:
    """Enterprise API deployment manager."""

    def __init__(self):
        self.deployments: Dict[str, DeploymentVersion] = {}
        self.health_checker = HealthChecker()
        self.traffic_router = TrafficRouter()
        self.deployment_history: List[Dict[str, Any]] = []

    async def deploy_version(self, version: str, config: Dict[str, Any],
                           strategy: str = "blue_green") -> DeploymentVersion:
        """Deploy a new API version."""
        deployment_id = f"deploy_{version}_{uuid.uuid4().hex[:8]}"

        logger.info(f"Starting deployment {deployment_id} for version {version}")

        # Create deployment
        deployment = DeploymentVersion(
            version=version,
            deployment_id=deployment_id,
            state=DeploymentState.DEPLOYING,
            config=config,
            instances=self._create_instances(config)
        )

        self.deployments[deployment_id] = deployment

        try:
            # Execute deployment strategy
            if strategy == "blue_green":
                await self._blue_green_deployment(deployment)
            elif strategy == "canary":
                await self._canary_deployment(deployment)
            elif strategy == "rolling":
                await self._rolling_deployment(deployment)
            else:
                await self._immediate_deployment(deployment)

            # Register with traffic router
            self.traffic_router.register_deployment(deployment)

            # Start health monitoring
            await self.health_checker.start_monitoring({deployment_id: deployment})

            # Record deployment
            self._record_deployment(deployment, "success")

            logger.info(f"Deployment {deployment_id} completed successfully")
            return deployment

        except Exception as e:
            deployment.state = DeploymentState.FAILED
            self._record_deployment(deployment, "failed", str(e))
            logger.error(f"Deployment {deployment_id} failed: {e}")
            raise

    async def _blue_green_deployment(self, deployment: DeploymentVersion):
        """Execute blue-green deployment strategy."""
        logger.info(f"Executing blue-green deployment for {deployment.deployment_id}")

        # Deploy to green environment
        await self._deploy_instances(deployment)

        # Validate green deployment
        await self._validate_deployment(deployment)

        # Switch traffic
        deployment.traffic_weight = 100.0
        deployment.state = DeploymentState.ACTIVE

        # Drain old deployments
        await self._drain_old_deployments(deployment.version)

    async def _canary_deployment(self, deployment: DeploymentVersion):
        """Execute canary deployment strategy."""
        logger.info(f"Executing canary deployment for {deployment.deployment_id}")

        # Deploy canary instances
        await self._deploy_instances(deployment)

        # Gradual traffic increase
        traffic_stages = [5, 10, 25, 50, 100]
        for stage in traffic_stages:
            deployment.traffic_weight = float(stage)
            logger.info(f"Canary stage: {stage}% traffic")

            # Monitor for issues
            await asyncio.sleep(60)  # Wait between stages

            if deployment.health.status == HealthStatus.UNHEALTHY:
                raise Exception(f"Canary deployment failed at {stage}% stage")

        deployment.state = DeploymentState.ACTIVE
        await self._drain_old_deployments(deployment.version)

    async def _rolling_deployment(self, deployment: DeploymentVersion):
        """Execute rolling deployment strategy."""
        logger.info(f"Executing rolling deployment for {deployment.deployment_id}")

        # Deploy instances one by one
        for i, instance in enumerate(deployment.instances):
            await self._deploy_instance(instance, deployment)
            await asyncio.sleep(30)  # Wait between instance deployments

            # Validate instance health
            instance_health = await self.health_checker._check_instance_health(instance)
            if instance_health > 1000:  # More than 1 second response time
                raise Exception(f"Rolling deployment failed at instance {i}")

        deployment.state = DeploymentState.ACTIVE
        deployment.traffic_weight = 100.0

    async def _immediate_deployment(self, deployment: DeploymentVersion):
        """Execute immediate deployment strategy."""
        logger.info(f"Executing immediate deployment for {deployment.deployment_id}")

        # Stop old deployments
        await self._stop_old_deployments(deployment.version)

        # Deploy new instances
        await self._deploy_instances(deployment)

        # Quick validation
        await self._validate_deployment(deployment)

        deployment.state = DeploymentState.ACTIVE
        deployment.traffic_weight = 100.0

    async def _deploy_instances(self, deployment: DeploymentVersion):
        """Deploy all instances for a deployment."""
        logger.info(f"Deploying {len(deployment.instances)} instances for {deployment.deployment_id}")

        # Simulate instance deployment
        for instance in deployment.instances:
            await self._deploy_instance(instance, deployment)

    async def _deploy_instance(self, instance: str, deployment: DeploymentVersion):
        """Deploy individual instance."""
        logger.info(f"Deploying instance {instance}")
        await asyncio.sleep(1)  # Simulate deployment time

    async def _validate_deployment(self, deployment: DeploymentVersion):
        """Validate deployment health."""
        logger.info(f"Validating deployment {deployment.deployment_id}")

        # Perform health checks
        health_metrics = await self.health_checker._perform_health_check(deployment)

        if health_metrics.status == HealthStatus.UNHEALTHY:
            raise Exception("Deployment validation failed: unhealthy instances")

        deployment.health = health_metrics

    async def _drain_old_deployments(self, current_version: str):
        """Drain traffic from old deployments."""
        for deployment_id, deployment in self.deployments.items():
            if deployment.version != current_version and deployment.state == DeploymentState.ACTIVE:
                logger.info(f"Draining deployment {deployment_id}")
                deployment.state = DeploymentState.DRAINING
                deployment.traffic_weight = 0.0

                # Wait for existing requests to complete
                await asyncio.sleep(30)

                deployment.state = DeploymentState.INACTIVE
                self.traffic_router.unregister_deployment(deployment_id)

    async def _stop_old_deployments(self, current_version: str):
        """Stop old deployments immediately."""
        for deployment_id, deployment in self.deployments.items():
            if deployment.version != current_version:
                logger.info(f"Stopping deployment {deployment_id}")
                deployment.state = DeploymentState.INACTIVE
                deployment.traffic_weight = 0.0
                self.traffic_router.unregister_deployment(deployment_id)

    def _create_instances(self, config: Dict[str, Any]) -> List[str]:
        """Create instance identifiers."""
        instance_count = config.get("instance_count", 3)
        return [f"instance_{i}" for i in range(instance_count)]

    def _record_deployment(self, deployment: DeploymentVersion, status: str, error: str = ""):
        """Record deployment in history."""
        record = {
            "deployment_id": deployment.deployment_id,
            "version": deployment.version,
            "status": status,
            "deployed_at": deployment.deployed_at.isoformat(),
            "instances": len(deployment.instances),
            "strategy": deployment.config.get("strategy", "unknown"),
            "error": error
        }

        self.deployment_history.append(record)

    async def rollback_deployment(self, deployment_id: str) -> bool:
        """Rollback a deployment."""
        if deployment_id not in self.deployments:
            logger.error(f"Deployment {deployment_id} not found for rollback")
            return False

        deployment = self.deployments[deployment_id]
        rollback_version = deployment.rollback_version

        if not rollback_version:
            logger.error(f"No rollback version specified for deployment {deployment_id}")
            return False

        try:
            logger.info(f"Rolling back deployment {deployment_id} to version {rollback_version}")

            # Stop current deployment
            deployment.state = DeploymentState.DRAINING
            deployment.traffic_weight = 0.0

            # Find rollback deployment
            rollback_deployment = None
            for dep_id, dep in self.deployments.items():
                if dep.version == rollback_version and dep.state == DeploymentState.INACTIVE:
                    rollback_deployment = dep
                    break

            if rollback_deployment:
                # Reactivate rollback deployment
                rollback_deployment.state = DeploymentState.ACTIVE
                rollback_deployment.traffic_weight = 100.0
                self.traffic_router.register_deployment(rollback_deployment)
            else:
                # Deploy rollback version
                await self.deploy_version(rollback_version, deployment.config, "immediate")

            # Mark original deployment as rolled back
            deployment.state = DeploymentState.ROLLED_BACK
            self.traffic_router.unregister_deployment(deployment_id)

            logger.info(f"Rollback completed for deployment {deployment_id}")
            return True

        except Exception as e:
            logger.error(f"Rollback failed for deployment {deployment_id}: {e}")
            return False

    def get_deployment_status(self) -> Dict[str, Any]:
        """Get status of all deployments."""
        return {
            "active_deployments": len([d for d in self.deployments.values()
                                     if d.state == DeploymentState.ACTIVE]),
            "deployments": {
                deployment_id: {
                    "version": deployment.version,
                    "state": deployment.state.value,
                    "traffic_weight": deployment.traffic_weight,
                    "health_status": deployment.health.status.value,
                    "deployed_at": deployment.deployed_at.isoformat(),
                    "instances": len(deployment.instances)
                }
                for deployment_id, deployment in self.deployments.items()
            },
            "traffic_distribution": self.traffic_router.get_traffic_distribution()
        }

    def get_deployment_history(self, limit: int = 20) -> List[Dict[str, Any]]:
        """Get deployment history."""
        return sorted(
            self.deployment_history,
            key=lambda x: x["deployed_at"],
            reverse=True
        )[:limit]


class DeploymentMiddleware(BaseHTTPMiddleware):
    """Middleware for handling deployment routing."""

    def __init__(self, app: FastAPI, deployment_manager: DeploymentManager):
        super().__init__(app)
        self.deployment_manager = deployment_manager

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Route request through deployment manager."""
        try:
            # Route request to appropriate deployment
            deployment_id = await self.deployment_manager.traffic_router.route_request(request)

            # Add deployment info to request
            request.state.deployment_id = deployment_id
            request.state.deployment_version = self.deployment_manager.deployments[deployment_id].version

            # Process request
            response = await call_next(request)

            # Add deployment headers
            response.headers["X-Deployment-ID"] = deployment_id
            response.headers["X-API-Version"] = request.state.deployment_version

            # Record successful request
            if deployment_id in self.deployment_manager.deployments:
                deployment = self.deployment_manager.deployments[deployment_id]
                deployment.health.success_count += 1

            return response

        except Exception as e:
            logger.error(f"Deployment routing failed: {e}")

            # Record failed request
            if hasattr(request.state, 'deployment_id'):
                deployment_id = request.state.deployment_id
                if deployment_id in self.deployment_manager.deployments:
                    deployment = self.deployment_manager.deployments[deployment_id]
                    deployment.health.error_count += 1

            raise HTTPException(status_code=503, detail="Service temporarily unavailable")


# Global deployment manager instance
deployment_manager = DeploymentManager()