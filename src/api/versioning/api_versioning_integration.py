"""
API Versioning Integration Hub
=============================

Enterprise integration module that combines version management, migration engine,
and deployment manager for comprehensive API versioning with backward compatibility.
"""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Callable
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request, Response, HTTPException, Depends
from fastapi.routing import APIRoute
from fastapi.middleware.base import BaseHTTPMiddleware

from .version_manager import (
    APIVersionManager,
    APIVersion,
    DeprecationLevel,
    VersioningStrategy,
    create_enterprise_version_manager
)
from .migration_engine import (
    MigrationOrchestrator,
    MigrationStrategy,
    migration_orchestrator
)
from .deployment_manager import (
    DeploymentManager,
    DeploymentMiddleware,
    deployment_manager
)
from core.logging import get_logger

logger = get_logger(__name__)


class VersionedAPIManager:
    """
    Comprehensive API versioning manager that integrates all versioning components.
    """

    def __init__(self, app: FastAPI):
        self.app = app
        self.version_manager = create_enterprise_version_manager()
        self.migration_orchestrator = migration_orchestrator
        self.deployment_manager = deployment_manager

        # Version-specific route handlers
        self.version_handlers: Dict[str, Dict[str, Callable]] = {}

        # Compatibility middleware
        self.compatibility_middleware = CompatibilityMiddleware(self.version_manager)

        # Initialize versioning system
        self._setup_versioning()

    def _setup_versioning(self):
        """Initialize the versioning system."""
        logger.info("Setting up enterprise API versioning system")

        # Add versioning middleware
        self.app.add_middleware(DeploymentMiddleware, deployment_manager=self.deployment_manager)
        self.app.add_middleware(CompatibilityMiddleware, version_manager=self.version_manager)

        # Add version management endpoints
        self._add_version_management_endpoints()

        # Add migration endpoints
        self._add_migration_endpoints()

        # Add deployment endpoints
        self._add_deployment_endpoints()

    def register_versioned_endpoint(
        self,
        path: str,
        methods: List[str],
        version_handlers: Dict[str, Callable],
        tags: List[str] = None
    ):
        """Register a versioned endpoint with handlers for different API versions."""

        async def versioned_handler(request: Request):
            # Extract version from request
            version, version_info = self.version_manager.extract_version(request)

            # Validate version
            version_obj = self.version_manager.validate_version(version)

            # Get appropriate handler
            if version in version_handlers:
                handler = version_handlers[version]
            else:
                # Find best compatible handler
                available_versions = sorted(version_handlers.keys(),
                                          key=lambda v: self._version_key(v),
                                          reverse=True)
                handler = version_handlers[available_versions[0]]
                logger.info(f"Using compatible handler {available_versions[0]} for requested version {version}")

            # Add version context to request
            request.state.api_version = version
            request.state.api_version_obj = version_obj
            request.state.version_info = version_info

            # Call handler
            response = await handler(request)

            # Add version headers
            self._add_version_headers(response, version, version_obj)

            return response

        # Register route for each method
        for method in methods:
            self.app.add_api_route(
                path=path,
                endpoint=versioned_handler,
                methods=[method],
                tags=tags or ["versioned"]
            )

        logger.info(f"Registered versioned endpoint: {method} {path} with versions {list(version_handlers.keys())}")

    def _version_key(self, version: str) -> tuple:
        """Convert version string to tuple for sorting."""
        parts = version.split('.')
        return tuple(int(part) for part in parts)

    def _add_version_headers(self, response: Response, version: str, version_obj: APIVersion):
        """Add version-related headers to response."""
        response.headers["API-Version"] = version
        response.headers["Supported-Versions"] = ",".join(self.version_manager.versions.keys())

        # Add deprecation headers if needed
        deprecation_headers = self.version_manager.get_deprecation_headers(version)
        for key, value in deprecation_headers.items():
            response.headers[key] = value

    def _add_version_management_endpoints(self):
        """Add version management API endpoints."""

        @self.app.get("/api/version/info", tags=["version-management"])
        async def get_version_info():
            """Get comprehensive version information."""
            return self.version_manager.get_version_info()

        @self.app.get("/api/version/migration-plan/{from_version}/{to_version}", tags=["version-management"])
        async def create_migration_plan(from_version: str, to_version: str):
            """Create migration plan between versions."""
            return self.version_manager.create_migration_summary(from_version, to_version)

        @self.app.post("/api/version/deprecate/{version}", tags=["version-management"])
        async def deprecate_version(version: str, deprecation_period_days: int = 180):
            """Deprecate an API version."""
            try:
                self.version_manager.schedule_deprecation(version, deprecation_period_days)
                return {
                    "status": "success",
                    "message": f"Version {version} scheduled for deprecation",
                    "deprecation_period_days": deprecation_period_days
                }
            except ValueError as e:
                raise HTTPException(status_code=400, detail=str(e))

    def _add_migration_endpoints(self):
        """Add migration management API endpoints."""

        @self.app.post("/api/migration/plan", tags=["migration-management"])
        async def plan_migration(
            from_version: str,
            to_version: str,
            strategy: str = "blue_green"
        ):
            """Plan a migration between API versions."""
            try:
                migration_strategy = MigrationStrategy(strategy)
                plan = await self.migration_orchestrator.plan_migration(
                    from_version, to_version, migration_strategy
                )
                return plan
            except ValueError as e:
                raise HTTPException(status_code=400, detail=str(e))

        @self.app.post("/api/migration/execute", tags=["migration-management"])
        async def execute_migration(plan: Dict[str, Any]):
            """Execute a migration plan."""
            result = await self.migration_orchestrator.execute_migration_plan(plan)
            return result

        @self.app.get("/api/migration/status/{migration_id}", tags=["migration-management"])
        async def get_migration_status(migration_id: str):
            """Get status of a specific migration."""
            status = await self.migration_orchestrator.get_migration_status(migration_id)
            return status

        @self.app.post("/api/migration/rollback/{migration_id}", tags=["migration-management"])
        async def rollback_migration(migration_id: str):
            """Rollback a specific migration."""
            result = await self.migration_orchestrator.rollback_migration(migration_id)
            return result

        @self.app.get("/api/migration/history", tags=["migration-management"])
        async def get_migration_history(limit: int = 50):
            """Get migration history."""
            history = self.migration_orchestrator.get_migration_history(limit)
            return {"migrations": history}

    def _add_deployment_endpoints(self):
        """Add deployment management API endpoints."""

        @self.app.post("/api/deployment/deploy", tags=["deployment-management"])
        async def deploy_version(
            version: str,
            strategy: str = "blue_green",
            instance_count: int = 3
        ):
            """Deploy a new API version."""
            config = {
                "strategy": strategy,
                "instance_count": instance_count
            }

            deployment = await self.deployment_manager.deploy_version(version, config, strategy)
            return {
                "deployment_id": deployment.deployment_id,
                "version": deployment.version,
                "status": deployment.state.value
            }

        @self.app.get("/api/deployment/status", tags=["deployment-management"])
        async def get_deployment_status():
            """Get status of all deployments."""
            return self.deployment_manager.get_deployment_status()

        @self.app.post("/api/deployment/rollback/{deployment_id}", tags=["deployment-management"])
        async def rollback_deployment(deployment_id: str):
            """Rollback a specific deployment."""
            success = await self.deployment_manager.rollback_deployment(deployment_id)
            return {
                "status": "success" if success else "failed",
                "deployment_id": deployment_id
            }

        @self.app.get("/api/deployment/history", tags=["deployment-management"])
        async def get_deployment_history(limit: int = 20):
            """Get deployment history."""
            history = self.deployment_manager.get_deployment_history(limit)
            return {"deployments": history}

        @self.app.post("/api/deployment/traffic-weights", tags=["deployment-management"])
        async def set_traffic_weights(weights: Dict[str, float]):
            """Set traffic weights for deployments."""
            try:
                self.deployment_manager.traffic_router.set_traffic_weights(weights)
                return {
                    "status": "success",
                    "weights": weights
                }
            except ValueError as e:
                raise HTTPException(status_code=400, detail=str(e))

    async def create_new_version(
        self,
        version: str,
        major: int,
        minor: int,
        patch: int = 0,
        changelog: str = "",
        breaking_changes: List[str] = None,
        migration_guide_url: str = None
    ) -> APIVersion:
        """Create and register a new API version."""

        api_version = APIVersion(
            version=version,
            major=major,
            minor=minor,
            patch=patch,
            release_date=datetime.utcnow(),
            changelog=changelog,
            breaking_changes=breaking_changes or [],
            migration_guide_url=migration_guide_url
        )

        self.version_manager.register_version(api_version)

        logger.info(f"Created new API version: {version}")
        return api_version

    async def automated_migration_workflow(
        self,
        from_version: str,
        to_version: str,
        strategy: MigrationStrategy = MigrationStrategy.BLUE_GREEN
    ) -> Dict[str, Any]:
        """Execute automated migration workflow with monitoring."""

        logger.info(f"Starting automated migration workflow: {from_version} -> {to_version}")

        try:
            # Step 1: Plan migration
            plan = await self.migration_orchestrator.plan_migration(from_version, to_version, strategy)

            if not plan["can_proceed"]:
                return {
                    "status": "blocked",
                    "reason": "Critical validation issues found",
                    "issues": plan["validation_issues"]
                }

            # Step 2: Deploy new version
            deployment = await self.deployment_manager.deploy_version(
                to_version,
                plan["config"],
                strategy.value
            )

            # Step 3: Execute migration
            migration_result = await self.migration_orchestrator.execute_migration_plan(plan)

            if migration_result["status"] == "success":
                return {
                    "status": "success",
                    "migration_id": migration_result["migration_id"],
                    "deployment_id": deployment.deployment_id,
                    "metrics": migration_result["metrics"]
                }
            else:
                # Rollback deployment on migration failure
                await self.deployment_manager.rollback_deployment(deployment.deployment_id)
                return {
                    "status": "failed",
                    "error": migration_result["error"],
                    "rollback_completed": True
                }

        except Exception as e:
            logger.error(f"Automated migration workflow failed: {e}")
            return {
                "status": "error",
                "error": str(e)
            }

    def get_version_compatibility_matrix(self) -> Dict[str, Any]:
        """Get compatibility matrix between different API versions."""
        versions = list(self.version_manager.versions.keys())
        matrix = {}

        for from_version in versions:
            matrix[from_version] = {}
            for to_version in versions:
                if from_version == to_version:
                    matrix[from_version][to_version] = "identical"
                else:
                    from_info = self.version_manager.versions[from_version]
                    to_info = self.version_manager.versions[to_version]

                    if from_info.major != to_info.major:
                        matrix[from_version][to_version] = "major_change"
                    elif from_info.minor != to_info.minor:
                        matrix[from_version][to_version] = "minor_change"
                    else:
                        matrix[from_version][to_version] = "patch_change"

        return {
            "versions": versions,
            "compatibility_matrix": matrix,
            "rules": [
                {
                    "from_version": rule.from_version,
                    "to_version": rule.to_version,
                    "field_mappings": rule.field_mappings,
                    "deprecated_fields": rule.deprecated_fields
                }
                for rule in self.version_manager.compatibility_rules
            ]
        }


class CompatibilityMiddleware(BaseHTTPMiddleware):
    """Middleware to handle backward compatibility transformations."""

    def __init__(self, app: FastAPI, version_manager: APIVersionManager):
        super().__init__(app)
        self.version_manager = version_manager

    async def dispatch(self, request: Request, call_next: Callable) -> Response:
        """Apply compatibility transformations to requests and responses."""

        # Extract version information
        version, version_info = self.version_manager.extract_version(request)

        # Store original request data for potential transformation
        if request.method in ["POST", "PUT", "PATCH"]:
            body = await request.body()
            if body:
                try:
                    request_data = json.loads(body)
                    request.state.original_request_data = request_data
                except json.JSONDecodeError:
                    pass

        # Process request
        response = await call_next(request)

        # Apply response transformations if needed
        if hasattr(request.state, 'api_version') and response.status_code == 200:
            try:
                response_body = b""
                async for chunk in response.body_iterator:
                    response_body += chunk

                if response_body:
                    response_data = json.loads(response_body)

                    # Check if transformation is needed
                    latest_version = self.version_manager.default_version
                    if request.state.api_version != latest_version:
                        # Transform response for backward compatibility
                        transformed_data = self.version_manager.transform_response_for_compatibility(
                            response_data,
                            latest_version,
                            request.state.api_version
                        )

                        # Create new response with transformed data
                        new_response = Response(
                            content=json.dumps(transformed_data),
                            status_code=response.status_code,
                            headers=dict(response.headers),
                            media_type="application/json"
                        )

                        return new_response

            except Exception as e:
                logger.error(f"Response transformation failed: {e}")
                # Return original response on transformation failure

        return response


# Factory function to create integrated versioning system
def create_versioned_api(app: FastAPI) -> VersionedAPIManager:
    """Create and configure integrated API versioning system."""
    return VersionedAPIManager(app)


# Example usage and integration helper
class APIVersioningIntegration:
    """Helper class for easy integration of API versioning."""

    @staticmethod
    def setup_versioning(app: FastAPI) -> VersionedAPIManager:
        """Set up comprehensive API versioning for a FastAPI application."""

        # Create versioned API manager
        versioned_api = create_versioned_api(app)

        # Add startup event to initialize versioning
        @app.on_event("startup")
        async def startup_versioning():
            logger.info("Initializing API versioning system")

            # Start health monitoring
            await versioned_api.deployment_manager.health_checker.start_monitoring(
                versioned_api.deployment_manager.deployments
            )

        # Add shutdown event to cleanup
        @app.on_event("shutdown")
        async def shutdown_versioning():
            logger.info("Shutting down API versioning system")

            # Stop health monitoring
            await versioned_api.deployment_manager.health_checker.stop_monitoring()

        return versioned_api

    @staticmethod
    def register_sales_api_versions(versioned_api: VersionedAPIManager):
        """Register sales API endpoints with versioning."""

        # Version 1.0 handler
        async def sales_v1_handler(request: Request):
            # Implementation for sales API v1.0
            return {
                "version": "1.0",
                "data": {
                    "invoice_date": "01/01/2024",
                    "customer_country": "UK",
                    "total_amount": 100.0
                }
            }

        # Version 2.0 handler
        async def sales_v2_handler(request: Request):
            # Implementation for sales API v2.0
            return {
                "version": "2.0",
                "data": {
                    "transaction_date": "2024-01-01",
                    "country_code": "GB",
                    "total_amount": 100.0
                }
            }

        # Register versioned endpoint
        versioned_api.register_versioned_endpoint(
            path="/api/sales",
            methods=["GET", "POST"],
            version_handlers={
                "1.0": sales_v1_handler,
                "2.0": sales_v2_handler
            },
            tags=["sales"]
        )

        logger.info("Registered sales API with versioning support")


# Global integration instance
api_versioning_integration = APIVersioningIntegration()