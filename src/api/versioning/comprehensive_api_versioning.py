"""
Comprehensive API Versioning Strategy
Enterprise-grade API versioning with backward compatibility, deprecation management,
automatic migration, and comprehensive version lifecycle management.
"""
from __future__ import annotations

import inspect
import json
import re
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

from fastapi import FastAPI, Request, Response, HTTPException
from fastapi.routing import APIRoute
from pydantic import BaseModel, Field
from starlette.middleware.base import BaseHTTPMiddleware

from core.logging import get_logger

logger = get_logger(__name__)


class VersioningStrategy(Enum):
    """API versioning strategies."""
    HEADER = "header"           # Version in Accept header
    URL_PATH = "url_path"       # Version in URL path (/v1/users)
    QUERY_PARAM = "query_param" # Version in query parameter (?version=1.0)
    CONTENT_TYPE = "content_type"  # Version in Content-Type header
    CUSTOM_HEADER = "custom_header"  # Custom version header


class VersionStatus(Enum):
    """Version lifecycle status."""
    DEVELOPMENT = "development"     # In development, not released
    BETA = "beta"                  # Beta release for testing
    STABLE = "stable"              # Stable production release
    DEPRECATED = "deprecated"      # Deprecated, still supported
    SUNSET = "sunset"              # Scheduled for removal
    RETIRED = "retired"            # No longer supported


class CompatibilityLevel(Enum):
    """Compatibility levels for version changes."""
    PATCH = "patch"        # Bug fixes, no breaking changes
    MINOR = "minor"        # New features, backward compatible
    MAJOR = "major"        # Breaking changes
    EXPERIMENTAL = "experimental"  # Experimental features


@dataclass
class ApiVersion:
    """API version definition with metadata."""
    version: str
    status: VersionStatus
    release_date: datetime
    deprecation_date: Optional[datetime] = None
    sunset_date: Optional[datetime] = None
    retirement_date: Optional[datetime] = None
    compatibility_level: CompatibilityLevel = CompatibilityLevel.MAJOR
    description: str = ""
    breaking_changes: List[str] = field(default_factory=list)
    new_features: List[str] = field(default_factory=list)
    bug_fixes: List[str] = field(default_factory=list)
    migration_guide_url: Optional[str] = None
    supported_until: Optional[datetime] = None


@dataclass
class EndpointVersion:
    """Version-specific endpoint configuration."""
    version: str
    handler: Callable
    path: str
    methods: Set[str]
    deprecated: bool = False
    deprecation_message: Optional[str] = None
    replacement_endpoint: Optional[str] = None
    custom_headers: Dict[str, str] = field(default_factory=dict)


class VersionCompatibilityRule(BaseModel):
    """Rule for handling version compatibility."""
    from_version: str = Field(..., description="Source version")
    to_version: str = Field(..., description="Target version")
    transformation_function: Optional[str] = Field(None, description="Function to transform data")
    field_mappings: Dict[str, str] = Field(default_factory=dict, description="Field name mappings")
    removed_fields: List[str] = Field(default_factory=list, description="Fields removed in new version")
    added_fields: Dict[str, Any] = Field(default_factory=dict, description="Fields added with default values")
    validation_rules: List[str] = Field(default_factory=list, description="Additional validation rules")


class VersionDetector:
    """Detects API version from requests using multiple strategies."""

    def __init__(self, strategies: List[VersioningStrategy], default_version: str = "1.0"):
        """Initialize version detector with supported strategies."""
        self.strategies = strategies
        self.default_version = default_version

    def detect_version(self, request: Request) -> str:
        """Detect version from request using configured strategies."""
        for strategy in self.strategies:
            version = self._detect_by_strategy(request, strategy)
            if version:
                return version

        return self.default_version

    def _detect_by_strategy(self, request: Request, strategy: VersioningStrategy) -> Optional[str]:
        """Detect version using specific strategy."""
        if strategy == VersioningStrategy.HEADER:
            # Extract from Accept header: Accept: application/vnd.api+json;version=1.0
            accept_header = request.headers.get("accept", "")
            version_match = re.search(r'version=([0-9.]+)', accept_header)
            return version_match.group(1) if version_match else None

        elif strategy == VersioningStrategy.URL_PATH:
            # Extract from URL path: /v1/users or /api/v2.1/users
            path = request.url.path
            version_match = re.search(r'/v?([0-9.]+)/', path)
            return version_match.group(1) if version_match else None

        elif strategy == VersioningStrategy.QUERY_PARAM:
            # Extract from query parameter: ?version=1.0 or ?api_version=2.0
            return (request.query_params.get("version") or
                   request.query_params.get("api_version"))

        elif strategy == VersioningStrategy.CONTENT_TYPE:
            # Extract from Content-Type: application/vnd.api.v1+json
            content_type = request.headers.get("content-type", "")
            version_match = re.search(r'\.v([0-9.]+)\+', content_type)
            return version_match.group(1) if version_match else None

        elif strategy == VersioningStrategy.CUSTOM_HEADER:
            # Extract from custom headers
            return (request.headers.get("x-api-version") or
                   request.headers.get("api-version"))

        return None


class DataTransformer:
    """Transforms data between different API versions."""

    def __init__(self):
        """Initialize data transformer."""
        self.transformation_rules: Dict[Tuple[str, str], VersionCompatibilityRule] = {}

    def add_compatibility_rule(self, rule: VersionCompatibilityRule):
        """Add a version compatibility rule."""
        key = (rule.from_version, rule.to_version)
        self.transformation_rules[key] = rule

        logger.info(f"Added compatibility rule: {rule.from_version} -> {rule.to_version}")

    def transform_request(self, data: Dict[str, Any], from_version: str, to_version: str) -> Dict[str, Any]:
        """Transform request data from one version to another."""
        rule = self.transformation_rules.get((from_version, to_version))
        if not rule:
            return data  # No transformation needed

        transformed_data = data.copy()

        # Apply field mappings
        for old_field, new_field in rule.field_mappings.items():
            if old_field in transformed_data:
                transformed_data[new_field] = transformed_data.pop(old_field)

        # Remove deprecated fields
        for field in rule.removed_fields:
            transformed_data.pop(field, None)

        # Add new fields with default values
        for field, default_value in rule.added_fields.items():
            if field not in transformed_data:
                transformed_data[field] = default_value

        # Apply custom transformation function if specified
        if rule.transformation_function:
            transformation_func = self._get_transformation_function(rule.transformation_function)
            if transformation_func:
                transformed_data = transformation_func(transformed_data)

        return transformed_data

    def transform_response(self, data: Dict[str, Any], from_version: str, to_version: str) -> Dict[str, Any]:
        """Transform response data from one version to another."""
        # For responses, we typically transform from newer to older versions
        # to maintain backward compatibility
        rule = self.transformation_rules.get((from_version, to_version))
        if not rule:
            return data

        transformed_data = data.copy()

        # Reverse field mappings for responses
        reverse_mappings = {v: k for k, v in rule.field_mappings.items()}
        for new_field, old_field in reverse_mappings.items():
            if new_field in transformed_data:
                transformed_data[old_field] = transformed_data.pop(new_field)

        # Remove fields that didn't exist in the older version
        for field in rule.added_fields.keys():
            transformed_data.pop(field, None)

        return transformed_data

    def _get_transformation_function(self, function_name: str) -> Optional[Callable]:
        """Get transformation function by name."""
        # This would typically load from a registry of transformation functions
        # For now, return None to indicate no custom transformation
        return None


class VersionRegistry:
    """Registry for managing API versions and their endpoints."""

    def __init__(self):
        """Initialize version registry."""
        self.versions: Dict[str, ApiVersion] = {}
        self.endpoints: Dict[str, Dict[str, EndpointVersion]] = defaultdict(dict)
        self.compatibility_matrix: Dict[Tuple[str, str], bool] = {}

    def register_version(self, version: ApiVersion):
        """Register a new API version."""
        self.versions[version.version] = version
        logger.info(f"Registered API version {version.version} with status {version.status.value}")

    def register_endpoint(self, endpoint_version: EndpointVersion):
        """Register a version-specific endpoint."""
        endpoint_key = f"{endpoint_version.path}:{','.join(sorted(endpoint_version.methods))}"
        self.endpoints[endpoint_key][endpoint_version.version] = endpoint_version

        logger.info(f"Registered endpoint {endpoint_version.path} for version {endpoint_version.version}")

    def get_version(self, version: str) -> Optional[ApiVersion]:
        """Get version information."""
        return self.versions.get(version)

    def get_supported_versions(self) -> List[str]:
        """Get list of supported versions."""
        return [
            version for version, info in self.versions.items()
            if info.status not in [VersionStatus.RETIRED]
        ]

    def get_latest_version(self) -> Optional[str]:
        """Get the latest stable version."""
        stable_versions = [
            (version, info) for version, info in self.versions.items()
            if info.status == VersionStatus.STABLE
        ]

        if not stable_versions:
            return None

        # Sort by release date and return the latest
        latest = max(stable_versions, key=lambda x: x[1].release_date)
        return latest[0]

    def get_endpoint_for_version(self, path: str, methods: Set[str], version: str) -> Optional[EndpointVersion]:
        """Get endpoint handler for specific version."""
        endpoint_key = f"{path}:{','.join(sorted(methods))}"
        return self.endpoints[endpoint_key].get(version)

    def get_available_versions_for_endpoint(self, path: str, methods: Set[str]) -> List[str]:
        """Get available versions for an endpoint."""
        endpoint_key = f"{path}:{','.join(sorted(methods))}"
        return list(self.endpoints[endpoint_key].keys())

    def is_version_compatible(self, from_version: str, to_version: str) -> bool:
        """Check if versions are compatible."""
        return self.compatibility_matrix.get((from_version, to_version), False)

    def set_compatibility(self, from_version: str, to_version: str, compatible: bool = True):
        """Set compatibility between versions."""
        self.compatibility_matrix[(from_version, to_version)] = compatible

    def get_deprecation_info(self) -> Dict[str, Any]:
        """Get information about deprecated versions and endpoints."""
        deprecated_versions = [
            (version, info) for version, info in self.versions.items()
            if info.status in [VersionStatus.DEPRECATED, VersionStatus.SUNSET]
        ]

        deprecated_endpoints = []
        for endpoint_key, versions in self.endpoints.items():
            for version, endpoint_info in versions.items():
                if endpoint_info.deprecated:
                    deprecated_endpoints.append({
                        'endpoint': endpoint_key,
                        'version': version,
                        'deprecation_message': endpoint_info.deprecation_message,
                        'replacement': endpoint_info.replacement_endpoint
                    })

        return {
            'deprecated_versions': [
                {
                    'version': version,
                    'status': info.status.value,
                    'deprecation_date': info.deprecation_date.isoformat() if info.deprecation_date else None,
                    'sunset_date': info.sunset_date.isoformat() if info.sunset_date else None,
                    'retirement_date': info.retirement_date.isoformat() if info.retirement_date else None
                }
                for version, info in deprecated_versions
            ],
            'deprecated_endpoints': deprecated_endpoints
        }


class ApiVersioningMiddleware(BaseHTTPMiddleware):
    """
    Comprehensive API versioning middleware that handles:
    - Version detection from multiple sources
    - Backward compatibility transformations
    - Deprecation warnings
    - Version routing
    """

    def __init__(
        self,
        app: FastAPI,
        version_registry: VersionRegistry,
        version_detector: VersionDetector,
        data_transformer: DataTransformer,
        enable_auto_migration: bool = True,
        enable_deprecation_warnings: bool = True
    ):
        """Initialize API versioning middleware."""
        super().__init__(app)
        self.version_registry = version_registry
        self.version_detector = version_detector
        self.data_transformer = data_transformer
        self.enable_auto_migration = enable_auto_migration
        self.enable_deprecation_warnings = enable_deprecation_warnings

        # Metrics
        self.version_usage_stats = defaultdict(int)
        self.deprecated_endpoint_usage = defaultdict(int)

    async def dispatch(self, request: Request, call_next):
        """Process request with version handling."""
        # Detect API version
        requested_version = self.version_detector.detect_version(request)

        # Get version info
        version_info = self.version_registry.get_version(requested_version)
        if not version_info:
            # Fallback to latest version if requested version doesn't exist
            latest_version = self.version_registry.get_latest_version()
            if latest_version:
                requested_version = latest_version
                version_info = self.version_registry.get_version(latest_version)

        # Update usage statistics
        self.version_usage_stats[requested_version] += 1

        # Check if version is retired
        if version_info and version_info.status == VersionStatus.RETIRED:
            raise HTTPException(
                status_code=410,
                detail=f"API version {requested_version} has been retired. "
                       f"Please upgrade to a supported version."
            )

        # Add version context to request
        request.state.api_version = requested_version
        request.state.version_info = version_info

        # Process request
        response = await call_next(request)

        # Add version headers to response
        response.headers["X-API-Version"] = requested_version
        response.headers["X-API-Supported-Versions"] = ",".join(
            self.version_registry.get_supported_versions()
        )

        # Add deprecation warnings if applicable
        if (version_info and
            version_info.status in [VersionStatus.DEPRECATED, VersionStatus.SUNSET] and
            self.enable_deprecation_warnings):

            self._add_deprecation_headers(response, version_info)

        return response

    def _add_deprecation_headers(self, response: Response, version_info: ApiVersion):
        """Add deprecation headers to response."""
        response.headers["Warning"] = f'299 - "API version {version_info.version} is deprecated"'

        if version_info.sunset_date:
            response.headers["Sunset"] = version_info.sunset_date.strftime("%a, %d %b %Y %H:%M:%S GMT")

        if version_info.migration_guide_url:
            response.headers["Link"] = f'<{version_info.migration_guide_url}>; rel="migration-guide"'

        # Add latest version info
        latest_version = self.version_registry.get_latest_version()
        if latest_version:
            response.headers["X-API-Latest-Version"] = latest_version


class ComprehensiveVersionManager:
    """
    Comprehensive API version manager that orchestrates all versioning concerns:
    - Version lifecycle management
    - Endpoint versioning
    - Data transformation
    - Deprecation management
    - Migration assistance
    """

    def __init__(
        self,
        app: FastAPI,
        versioning_strategies: List[VersioningStrategy] = None,
        default_version: str = "1.0"
    ):
        """Initialize comprehensive version manager."""
        self.app = app
        self.default_version = default_version

        # Initialize components
        self.version_registry = VersionRegistry()
        self.version_detector = VersionDetector(
            versioning_strategies or [VersioningStrategy.HEADER, VersioningStrategy.URL_PATH],
            default_version
        )
        self.data_transformer = DataTransformer()

        # Add middleware
        self.middleware = ApiVersioningMiddleware(
            app, self.version_registry, self.version_detector, self.data_transformer
        )
        app.add_middleware(ApiVersioningMiddleware, **self.middleware.__dict__)

        # Setup default versions
        self._setup_default_versions()

    def _setup_default_versions(self):
        """Setup default API versions."""
        # Version 1.0 - Initial stable release
        v1 = ApiVersion(
            version="1.0",
            status=VersionStatus.STABLE,
            release_date=datetime(2024, 1, 1),
            description="Initial stable release"
        )
        self.version_registry.register_version(v1)

        # Version 2.0 - Current stable release
        v2 = ApiVersion(
            version="2.0",
            status=VersionStatus.STABLE,
            release_date=datetime(2024, 6, 1),
            description="Enhanced API with improved performance and new features"
        )
        self.version_registry.register_version(v2)

        # Version 3.0 - Beta release
        v3 = ApiVersion(
            version="3.0",
            status=VersionStatus.BETA,
            release_date=datetime(2024, 12, 1),
            description="Next generation API with advanced features"
        )
        self.version_registry.register_version(v3)

        # Set compatibility
        self.version_registry.set_compatibility("1.0", "2.0", True)
        self.version_registry.set_compatibility("2.0", "3.0", True)

    def register_version(
        self,
        version: str,
        status: VersionStatus = VersionStatus.DEVELOPMENT,
        description: str = "",
        breaking_changes: List[str] = None,
        new_features: List[str] = None,
        **kwargs
    ):
        """Register a new API version."""
        api_version = ApiVersion(
            version=version,
            status=status,
            release_date=kwargs.get('release_date', datetime.utcnow()),
            deprecation_date=kwargs.get('deprecation_date'),
            sunset_date=kwargs.get('sunset_date'),
            retirement_date=kwargs.get('retirement_date'),
            description=description,
            breaking_changes=breaking_changes or [],
            new_features=new_features or [],
            migration_guide_url=kwargs.get('migration_guide_url')
        )

        self.version_registry.register_version(api_version)

    def version_endpoint(
        self,
        path: str,
        methods: List[str],
        version: str,
        deprecated: bool = False,
        deprecation_message: str = None,
        replacement_endpoint: str = None
    ):
        """Decorator for versioning endpoints."""
        def decorator(func):
            endpoint_version = EndpointVersion(
                version=version,
                handler=func,
                path=path,
                methods=set(methods),
                deprecated=deprecated,
                deprecation_message=deprecation_message,
                replacement_endpoint=replacement_endpoint
            )

            self.version_registry.register_endpoint(endpoint_version)
            return func

        return decorator

    def add_compatibility_rule(
        self,
        from_version: str,
        to_version: str,
        field_mappings: Dict[str, str] = None,
        removed_fields: List[str] = None,
        added_fields: Dict[str, Any] = None
    ):
        """Add version compatibility rule."""
        rule = VersionCompatibilityRule(
            from_version=from_version,
            to_version=to_version,
            field_mappings=field_mappings or {},
            removed_fields=removed_fields or [],
            added_fields=added_fields or {}
        )

        self.data_transformer.add_compatibility_rule(rule)

    def deprecate_version(
        self,
        version: str,
        deprecation_date: datetime = None,
        sunset_date: datetime = None,
        retirement_date: datetime = None,
        migration_guide_url: str = None
    ):
        """Deprecate an API version."""
        version_info = self.version_registry.get_version(version)
        if version_info:
            version_info.status = VersionStatus.DEPRECATED
            version_info.deprecation_date = deprecation_date or datetime.utcnow()
            version_info.sunset_date = sunset_date
            version_info.retirement_date = retirement_date
            version_info.migration_guide_url = migration_guide_url

            logger.info(f"Deprecated API version {version}")

    def get_version_info(self) -> Dict[str, Any]:
        """Get comprehensive version information."""
        return {
            'current_versions': {
                version: {
                    'status': info.status.value,
                    'release_date': info.release_date.isoformat(),
                    'description': info.description,
                    'breaking_changes': info.breaking_changes,
                    'new_features': info.new_features
                }
                for version, info in self.version_registry.versions.items()
            },
            'latest_version': self.version_registry.get_latest_version(),
            'supported_versions': self.version_registry.get_supported_versions(),
            'deprecation_info': self.version_registry.get_deprecation_info(),
            'usage_statistics': dict(self.middleware.version_usage_stats),
            'versioning_strategies': [strategy.value for strategy in self.version_detector.strategies]
        }

    def generate_migration_guide(self, from_version: str, to_version: str) -> Dict[str, Any]:
        """Generate migration guide between versions."""
        from_info = self.version_registry.get_version(from_version)
        to_info = self.version_registry.get_version(to_version)

        if not from_info or not to_info:
            return {'error': 'Invalid version specified'}

        # Get compatibility rules
        rule_key = (from_version, to_version)
        compatibility_rule = self.data_transformer.transformation_rules.get(rule_key)

        guide = {
            'from_version': from_version,
            'to_version': to_version,
            'compatibility_level': to_info.compatibility_level.value,
            'breaking_changes': to_info.breaking_changes,
            'new_features': to_info.new_features,
            'migration_steps': [],
            'field_changes': {},
            'recommendations': []
        }

        if compatibility_rule:
            guide['field_changes'] = {
                'field_mappings': compatibility_rule.field_mappings,
                'removed_fields': compatibility_rule.removed_fields,
                'added_fields': compatibility_rule.added_fields
            }

        # Add specific migration steps
        if to_info.compatibility_level == CompatibilityLevel.MAJOR:
            guide['migration_steps'].extend([
                'Review breaking changes listed below',
                'Update client code to handle new field names',
                'Remove usage of deprecated fields',
                'Test thoroughly with new version',
                'Update API version in requests'
            ])

        guide['recommendations'].extend([
            f'Read the migration guide: {to_info.migration_guide_url}' if to_info.migration_guide_url else 'Contact support for migration assistance',
            'Test in non-production environment first',
            'Monitor error rates during migration',
            'Consider gradual rollout strategy'
        ])

        return guide


# Export key classes and functions
__all__ = [
    'ComprehensiveVersionManager',
    'ApiVersioningMiddleware',
    'VersionRegistry',
    'VersionDetector',
    'DataTransformer',
    'ApiVersion',
    'EndpointVersion',
    'VersionCompatibilityRule',
    'VersioningStrategy',
    'VersionStatus',
    'CompatibilityLevel'
]