"""
Advanced API Versioning Manager
Enterprise-grade API versioning with backward compatibility, deprecation management, and migration support.
"""
from __future__ import annotations

import re
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable

from fastapi import FastAPI, HTTPException, Request
from fastapi.routing import APIRoute
from pydantic import BaseModel, Field


class VersioningStrategy(Enum):
    """API versioning strategies supported."""
    HEADER = "header"  # Accept-Version: v1
    PATH = "path"      # /api/v1/users
    QUERY = "query"    # /api/users?version=v1
    CONTENT_TYPE = "content_type"  # application/vnd.api.v1+json


class DeprecationLevel(Enum):
    """API deprecation levels."""
    STABLE = "stable"
    DEPRECATED = "deprecated"
    SUNSET = "sunset"
    OBSOLETE = "obsolete"


class APIVersion(BaseModel):
    """API version definition."""
    version: str = Field(..., description="Version string (e.g., '1.0', 'v2')")
    major: int = Field(..., description="Major version number")
    minor: int = Field(..., description="Minor version number")
    patch: int = Field(default=0, description="Patch version number")
    release_date: datetime = Field(..., description="Version release date")
    deprecation_date: datetime | None = Field(None, description="When version was deprecated")
    sunset_date: datetime | None = Field(None, description="When version will be removed")
    deprecation_level: DeprecationLevel = Field(default=DeprecationLevel.STABLE)
    changelog: str | None = Field(None, description="Version changelog")
    breaking_changes: list[str] = Field(default_factory=list, description="List of breaking changes")
    migration_guide_url: str | None = Field(None, description="URL to migration guide")


class BackwardCompatibilityRule(BaseModel):
    """Rule for maintaining backward compatibility."""
    from_version: str
    to_version: str
    field_mappings: dict[str, str] = Field(default_factory=dict)
    response_transformers: dict[str, Callable] = Field(default_factory=dict)
    request_transformers: dict[str, Callable] = Field(default_factory=dict)
    deprecated_fields: list[str] = Field(default_factory=list)


class VersionedEndpoint(BaseModel):
    """Versioned endpoint configuration."""
    path: str
    method: str
    supported_versions: list[str]
    default_version: str
    deprecated_versions: list[str] = Field(default_factory=list)
    sunset_versions: list[str] = Field(default_factory=list)
    version_specific_implementations: dict[str, Callable] = Field(default_factory=dict)


class APIVersionManager:
    """Enterprise API version manager with backward compatibility."""
    
    def __init__(self, strategy: VersioningStrategy = VersioningStrategy.PATH):
        self.strategy = strategy
        self.versions: dict[str, APIVersion] = {}
        self.compatibility_rules: list[BackwardCompatibilityRule] = []
        self.versioned_endpoints: dict[str, VersionedEndpoint] = {}
        self.default_version = "1.0"
        
        # Version header names
        self.version_headers = {
            "accept": "Accept-Version",
            "api": "API-Version",
            "custom": "X-API-Version"
        }
        
        # Content type patterns
        self.content_type_pattern = re.compile(r"application/vnd\.[\w\-\.]+\.v(\d+(?:\.\d+)?)(?:\+\w+)?")

    def register_version(self, version: APIVersion) -> None:
        """Register a new API version."""
        self.versions[version.version] = version
        
        # Set as default if it's the latest stable version
        if version.deprecation_level == DeprecationLevel.STABLE:
            current_default = self.versions.get(self.default_version)
            if not current_default or version.major > current_default.major:
                self.default_version = version.version

    def register_compatibility_rule(self, rule: BackwardCompatibilityRule) -> None:
        """Register backward compatibility rule."""
        self.compatibility_rules.append(rule)

    def register_endpoint(self, endpoint: VersionedEndpoint) -> None:
        """Register versioned endpoint."""
        key = f"{endpoint.method}:{endpoint.path}"
        self.versioned_endpoints[key] = endpoint

    def extract_version(self, request: Request) -> tuple[str, dict[str, Any]]:
        """Extract version from request based on configured strategy."""
        version_info = {"source": self.strategy.value, "raw_value": None}
        
        if self.strategy == VersioningStrategy.HEADER:
            # Check multiple header variations
            for header_name in self.version_headers.values():
                if header_name.lower() in request.headers:
                    version = request.headers[header_name.lower()]
                    version_info["raw_value"] = version
                    return self._normalize_version(version), version_info
                    
        elif self.strategy == VersioningStrategy.PATH:
            # Extract from path pattern /api/v1/... or /api/v1.2/...
            path = str(request.url.path)
            match = re.search(r"/api/v(\d+(?:\.\d+)?)", path)
            if match:
                version = match.group(1)
                version_info["raw_value"] = f"v{version}"
                return self._normalize_version(version), version_info
                
        elif self.strategy == VersioningStrategy.QUERY:
            # Extract from query parameter
            version = request.query_params.get("version") or request.query_params.get("v")
            if version:
                version_info["raw_value"] = version
                return self._normalize_version(version), version_info
                
        elif self.strategy == VersioningStrategy.CONTENT_TYPE:
            # Extract from Accept or Content-Type headers
            accept_header = request.headers.get("accept", "")
            content_type = request.headers.get("content-type", "")
            
            for header_value in [accept_header, content_type]:
                match = self.content_type_pattern.search(header_value)
                if match:
                    version = match.group(1)
                    version_info["raw_value"] = header_value
                    return self._normalize_version(version), version_info
        
        # Return default version if none specified
        version_info["source"] = "default"
        return self.default_version, version_info

    def _normalize_version(self, version: str) -> str:
        """Normalize version string to consistent format."""
        # Remove 'v' prefix if present
        version = version.lstrip('v')
        
        # Handle different version formats
        parts = version.split('.')
        if len(parts) == 1:
            return f"{parts[0]}.0"
        elif len(parts) == 2:
            return f"{parts[0]}.{parts[1]}"
        else:
            return f"{parts[0]}.{parts[1]}.{parts[2]}"

    def validate_version(self, version: str) -> APIVersion:
        """Validate and return version information."""
        if version not in self.versions:
            # Try to find the closest compatible version
            available_versions = list(self.versions.keys())
            raise HTTPException(
                status_code=400,
                detail=f"API version '{version}' not supported. Available versions: {available_versions}"
            )
        
        version_info = self.versions[version]
        
        # Check if version is obsolete
        if version_info.deprecation_level == DeprecationLevel.OBSOLETE:
            raise HTTPException(
                status_code=410,
                detail=f"API version '{version}' is no longer supported"
            )
        
        return version_info

    def get_deprecation_headers(self, version: str) -> dict[str, str]:
        """Get deprecation headers for response."""
        version_info = self.versions.get(version)
        if not version_info:
            return {}
        
        headers = {}
        
        if version_info.deprecation_level in [DeprecationLevel.DEPRECATED, DeprecationLevel.SUNSET]:
            headers["Deprecation"] = "true"
            
            if version_info.deprecation_date:
                headers["Sunset"] = version_info.deprecation_date.strftime("%a, %d %b %Y %H:%M:%S GMT")
            
            if version_info.sunset_date:
                headers["Sunset"] = version_info.sunset_date.strftime("%a, %d %b %Y %H:%M:%S GMT")
            
            # Add link to migration guide
            if version_info.migration_guide_url:
                headers["Link"] = f'<{version_info.migration_guide_url}>; rel="successor-version"'
            
            # Add warning message
            warning_msg = f"API version {version} is deprecated"
            if version_info.sunset_date:
                warning_msg += f" and will be removed on {version_info.sunset_date.date()}"
            headers["Warning"] = f'299 - "{warning_msg}"'
        
        return headers

    def transform_response_for_compatibility(
        self, 
        data: dict[str, Any], 
        from_version: str, 
        to_version: str
    ) -> dict[str, Any]:
        """Transform response data for backward compatibility."""
        for rule in self.compatibility_rules:
            if rule.from_version == from_version and rule.to_version == to_version:
                # Apply field mappings
                transformed_data = {}
                for key, value in data.items():
                    mapped_key = rule.field_mappings.get(key, key)
                    transformed_data[mapped_key] = value
                
                # Apply response transformers
                for field, transformer in rule.response_transformers.items():
                    if field in transformed_data:
                        transformed_data[field] = transformer(transformed_data[field])
                
                # Add deprecated field warnings
                for deprecated_field in rule.deprecated_fields:
                    if deprecated_field in transformed_data:
                        # Could add a warning header or log deprecated field usage
                        pass
                
                return transformed_data
        
        return data

    def create_versioned_route(
        self,
        app: FastAPI,
        path: str,
        methods: list[str],
        version_handlers: dict[str, Callable]
    ) -> None:
        """Create a versioned route that handles multiple API versions."""
        
        async def versioned_handler(request: Request):
            version, version_info = self.extract_version(request)
            version_obj = self.validate_version(version)
            
            # Get appropriate handler for this version
            if version in version_handlers:
                handler = version_handlers[version]
            else:
                # Try to find the best compatible handler
                available_versions = sorted(version_handlers.keys(), reverse=True)
                handler = version_handlers[available_versions[0]]
            
            # Call the handler
            response = await handler(request)
            
            # Add version headers
            response.headers["API-Version"] = version
            response.headers["Supported-Versions"] = ",".join(self.versions.keys())
            
            # Add deprecation headers if needed
            deprecation_headers = self.get_deprecation_headers(version)
            for key, value in deprecation_headers.items():
                response.headers[key] = value
            
            return response
        
        # Create the route
        for method in methods:
            app.add_api_route(
                path=path,
                endpoint=versioned_handler,
                methods=[method],
                tags=["versioned"]
            )

    def get_version_info(self) -> dict[str, Any]:
        """Get comprehensive version information."""
        return {
            "strategy": self.strategy.value,
            "default_version": self.default_version,
            "supported_versions": list(self.versions.keys()),
            "versions": {
                version: {
                    "version": info.version,
                    "major": info.major,
                    "minor": info.minor,
                    "patch": info.patch,
                    "release_date": info.release_date.isoformat(),
                    "deprecation_level": info.deprecation_level.value,
                    "deprecation_date": info.deprecation_date.isoformat() if info.deprecation_date else None,
                    "sunset_date": info.sunset_date.isoformat() if info.sunset_date else None,
                    "breaking_changes": info.breaking_changes,
                    "migration_guide_url": info.migration_guide_url
                }
                for version, info in self.versions.items()
            }
        }

    def schedule_deprecation(self, version: str, deprecation_period_days: int = 180) -> None:
        """Schedule a version for deprecation."""
        if version not in self.versions:
            raise ValueError(f"Version {version} not found")
        
        version_info = self.versions[version]
        version_info.deprecation_date = datetime.utcnow()
        version_info.sunset_date = datetime.utcnow() + timedelta(days=deprecation_period_days)
        version_info.deprecation_level = DeprecationLevel.DEPRECATED

    def create_migration_summary(self, from_version: str, to_version: str) -> dict[str, Any]:
        """Create a migration summary between versions."""
        if from_version not in self.versions or to_version not in self.versions:
            raise ValueError("Invalid version specified")
        
        from_info = self.versions[from_version]
        to_info = self.versions[to_version]
        
        # Find applicable compatibility rules
        applicable_rules = [
            rule for rule in self.compatibility_rules
            if rule.from_version == from_version and rule.to_version == to_version
        ]
        
        return {
            "from_version": from_version,
            "to_version": to_version,
            "breaking_changes": to_info.breaking_changes,
            "field_mappings": applicable_rules[0].field_mappings if applicable_rules else {},
            "deprecated_fields": applicable_rules[0].deprecated_fields if applicable_rules else [],
            "migration_guide_url": to_info.migration_guide_url,
            "estimated_effort": self._estimate_migration_effort(from_info, to_info)
        }

    def _estimate_migration_effort(self, from_version: APIVersion, to_version: APIVersion) -> str:
        """Estimate migration effort based on version differences."""
        if from_version.major != to_version.major:
            return "high"
        elif from_version.minor != to_version.minor and to_version.breaking_changes:
            return "medium"
        else:
            return "low"


# Factory function to create common version configurations
def create_enterprise_version_manager() -> APIVersionManager:
    """Create enterprise version manager with common configurations."""
    manager = APIVersionManager(strategy=VersioningStrategy.PATH)
    
    # Register current versions
    manager.register_version(APIVersion(
        version="1.0",
        major=1,
        minor=0,
        release_date=datetime(2024, 1, 1),
        changelog="Initial API release with core functionality",
        breaking_changes=[]
    ))
    
    manager.register_version(APIVersion(
        version="1.1",
        major=1,
        minor=1,
        release_date=datetime(2024, 3, 1),
        changelog="Enhanced filtering and pagination",
        breaking_changes=[]
    ))
    
    manager.register_version(APIVersion(
        version="2.0",
        major=2,
        minor=0,
        release_date=datetime(2024, 6, 1),
        changelog="Major API redesign with improved performance",
        breaking_changes=[
            "Response structure changed for sales endpoints",
            "Date format standardized to ISO 8601",
            "Authentication now requires Bearer tokens"
        ],
        migration_guide_url="/docs/migration/v1-to-v2"
    ))
    
    # Register compatibility rules
    manager.register_compatibility_rule(BackwardCompatibilityRule(
        from_version="1.0",
        to_version="2.0",
        field_mappings={
            "invoice_date": "transaction_date",
            "customer_country": "country"
        },
        deprecated_fields=["old_customer_id"]
    ))
    
    return manager


# Middleware for automatic version handling
class VersioningMiddleware:
    """Middleware to automatically handle API versioning."""
    
    def __init__(self, app: FastAPI, version_manager: APIVersionManager):
        self.app = app
        self.version_manager = version_manager

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            request = Request(scope, receive)
            
            # Extract and validate version
            try:
                version, version_info = self.version_manager.extract_version(request)
                version_obj = self.version_manager.validate_version(version)
                
                # Add version info to request state
                scope["state"]["api_version"] = version
                scope["state"]["api_version_info"] = version_info
                scope["state"]["api_version_obj"] = version_obj
                
            except HTTPException as e:
                # Send error response for invalid version
                response = Response(
                    content=f'{{"error": "{e.detail}"}}',
                    status_code=e.status_code,
                    headers={"content-type": "application/json"}
                )
                await response(scope, receive, send)
                return
        
        await self.app(scope, receive, send)


if __name__ == "__main__":
    # Example usage
    version_manager = create_enterprise_version_manager()
    
    print("Version Info:")
    print(version_manager.get_version_info())
    
    print("\nMigration Summary 1.0 -> 2.0:")
    print(version_manager.create_migration_summary("1.0", "2.0"))