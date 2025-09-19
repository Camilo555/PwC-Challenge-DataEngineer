"""
API Version Documentation Generator
==================================

Automatically generates comprehensive documentation for versioned APIs,
including migration guides, compatibility matrices, and interactive docs.
"""
from __future__ import annotations

import json
import yaml
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from jinja2 import Environment, FileSystemLoader, Template

from .version_manager import APIVersionManager, APIVersion, DeprecationLevel
from .migration_engine import MigrationOrchestrator, ValidationIssue
from core.logging import get_logger

logger = get_logger(__name__)


class DocumentationGenerator:
    """Generates comprehensive API version documentation."""

    def __init__(self, version_manager: APIVersionManager, migration_orchestrator: MigrationOrchestrator):
        self.version_manager = version_manager
        self.migration_orchestrator = migration_orchestrator
        self.output_dir = Path("docs/api/versions")
        self.templates_dir = Path("src/api/versioning/templates")

        # Ensure directories exist
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.templates_dir.mkdir(parents=True, exist_ok=True)

        # Initialize Jinja2 environment
        self.jinja_env = Environment(
            loader=FileSystemLoader(str(self.templates_dir)),
            autoescape=True
        )

        # Create templates if they don't exist
        self._create_templates()

    def _create_templates(self):
        """Create documentation templates if they don't exist."""

        # Version overview template
        version_overview_template = '''
# API Version {{ version.version }} Documentation

## Overview
- **Version**: {{ version.version }}
- **Release Date**: {{ version.release_date.strftime('%Y-%m-%d') }}
- **Status**: {{ version.deprecation_level.value.title() }}
{% if version.deprecation_date %}
- **Deprecated**: {{ version.deprecation_date.strftime('%Y-%m-%d') }}
{% endif %}
{% if version.sunset_date %}
- **Sunset Date**: {{ version.sunset_date.strftime('%Y-%m-%d') }}
{% endif %}

## Changelog
{{ version.changelog }}

{% if version.breaking_changes %}
## Breaking Changes
{% for change in version.breaking_changes %}
- {{ change }}
{% endfor %}
{% endif %}

{% if version.migration_guide_url %}
## Migration Guide
For detailed migration instructions, see: [Migration Guide]({{ version.migration_guide_url }})
{% endif %}

## API Endpoints
{% for endpoint in endpoints %}
### {{ endpoint.method }} {{ endpoint.path }}
{{ endpoint.description }}

**Parameters:**
{% for param in endpoint.parameters %}
- `{{ param.name }}` ({{ param.type }}): {{ param.description }}
{% endfor %}

**Response:**
```json
{{ endpoint.example_response | tojson(indent=2) }}
```
{% endfor %}
'''

        # Migration guide template
        migration_guide_template = '''
# Migration Guide: {{ from_version }} → {{ to_version }}

## Overview
This guide will help you migrate from API version {{ from_version }} to {{ to_version }}.

**Migration Effort**: {{ migration_summary.estimated_effort.title() }}

## Breaking Changes
{% for change in migration_summary.breaking_changes %}
- {{ change }}
{% endfor %}

## Field Mappings
{% if migration_summary.field_mappings %}
| Old Field ({{ from_version }}) | New Field ({{ to_version }}) |
|---|---|
{% for old_field, new_field in migration_summary.field_mappings.items() %}
| `{{ old_field }}` | `{{ new_field }}` |
{% endfor %}
{% endif %}

## Deprecated Fields
{% if migration_summary.deprecated_fields %}
The following fields are deprecated and will be removed in future versions:
{% for field in migration_summary.deprecated_fields %}
- `{{ field }}`
{% endfor %}
{% endif %}

## Code Examples

### Before ({{ from_version }})
```json
{{ examples.before | tojson(indent=2) }}
```

### After ({{ to_version }})
```json
{{ examples.after | tojson(indent=2) }}
```

## Testing Your Migration

1. **Validate your code** against the new API version
2. **Test backward compatibility** using the compatibility endpoints
3. **Monitor deprecation warnings** in API responses
4. **Update your client** to use the new field names

## Support
For migration support, contact the API team or create an issue in our repository.
'''

        # Compatibility matrix template
        compatibility_matrix_template = '''
# API Version Compatibility Matrix

## Overview
This matrix shows the compatibility between different API versions and available migration paths.

## Version Status
{% for version, info in versions.items() %}
- **{{ version }}**: {{ info.deprecation_level.value.title() }}
  {% if info.deprecation_date %}(Deprecated: {{ info.deprecation_date.strftime('%Y-%m-%d') }}){% endif %}
{% endfor %}

## Compatibility Matrix

| From \\ To | {% for version in versions.keys() %}{{ version }} | {% endfor %}
|---|{% for version in versions.keys() %}---|{% endfor %}
{% for from_version in versions.keys() %}
| **{{ from_version }}** | {% for to_version in versions.keys() %}{{ compatibility_matrix[from_version][to_version] }} | {% endfor %}
{% endfor %}

### Legend
- **identical**: Same version
- **patch_change**: Patch-level changes (backward compatible)
- **minor_change**: Minor version changes (mostly backward compatible)
- **major_change**: Major version changes (may have breaking changes)

## Migration Paths
{% for rule in compatibility_rules %}
### {{ rule.from_version }} → {{ rule.to_version }}
{% if rule.field_mappings %}
**Field Mappings:**
{% for old_field, new_field in rule.field_mappings.items() %}
- `{{ old_field }}` → `{{ new_field }}`
{% endfor %}
{% endif %}
{% if rule.deprecated_fields %}
**Deprecated Fields:** {{ rule.deprecated_fields | join(', ') }}
{% endif %}
{% endfor %}
'''

        # Write templates to files
        templates = {
            "version_overview.md": version_overview_template,
            "migration_guide.md": migration_guide_template,
            "compatibility_matrix.md": compatibility_matrix_template
        }

        for filename, content in templates.items():
            template_path = self.templates_dir / filename
            if not template_path.exists():
                template_path.write_text(content.strip())

    async def generate_version_documentation(self, version: str) -> Path:
        """Generate documentation for a specific API version."""
        logger.info(f"Generating documentation for API version {version}")

        if version not in self.version_manager.versions:
            raise ValueError(f"Version {version} not found")

        version_info = self.version_manager.versions[version]

        # Mock endpoints for demonstration
        endpoints = [
            {
                "method": "GET",
                "path": "/api/sales",
                "description": "Retrieve sales data with filtering and pagination",
                "parameters": [
                    {"name": "date_from", "type": "string", "description": "Start date for filtering"},
                    {"name": "date_to", "type": "string", "description": "End date for filtering"},
                    {"name": "country", "type": "string", "description": "Country filter"},
                    {"name": "page", "type": "integer", "description": "Page number for pagination"},
                    {"name": "page_size", "type": "integer", "description": "Number of items per page"}
                ],
                "example_response": {
                    "data": [
                        {
                            "invoice_date" if version == "1.0" else "transaction_date": "01/01/2024" if version == "1.0" else "2024-01-01",
                            "customer_country" if version == "1.0" else "country_code": "UK" if version == "1.0" else "GB",
                            "total_amount": 100.0,
                            "quantity": 2
                        }
                    ],
                    "pagination": {
                        "page": 1,
                        "page_size": 20,
                        "total_count": 100,
                        "has_next": True
                    }
                }
            },
            {
                "method": "POST",
                "path": "/api/sales",
                "description": "Create new sales record",
                "parameters": [
                    {"name": "customer_id", "type": "string", "description": "Customer identifier"},
                    {"name": "product_id", "type": "string", "description": "Product identifier"},
                    {"name": "quantity", "type": "integer", "description": "Quantity sold"},
                    {"name": "unit_price", "type": "number", "description": "Price per unit"}
                ],
                "example_response": {
                    "id": "sale_123",
                    "status": "created",
                    "invoice_date" if version == "1.0" else "transaction_date": "01/01/2024" if version == "1.0" else "2024-01-01"
                }
            }
        ]

        # Load template
        template = self.jinja_env.get_template("version_overview.md")

        # Render documentation
        content = template.render(
            version=version_info,
            endpoints=endpoints,
            generated_at=datetime.utcnow()
        )

        # Write to file
        output_path = self.output_dir / f"v{version}.md"
        output_path.write_text(content)

        logger.info(f"Generated version documentation: {output_path}")
        return output_path

    async def generate_migration_guide(self, from_version: str, to_version: str) -> Path:
        """Generate migration guide between two versions."""
        logger.info(f"Generating migration guide: {from_version} → {to_version}")

        # Get migration summary
        migration_summary = self.version_manager.create_migration_summary(from_version, to_version)

        # Create example data showing before/after
        examples = {
            "before": {
                "invoice_date": "01/01/2024",
                "customer_country": "UK",
                "product_id": "PROD123",
                "total_amount": 100.0
            },
            "after": {
                "transaction_date": "2024-01-01",
                "country_code": "GB",
                "stock_code": "PROD123",
                "total_amount": 100.0
            }
        }

        # Load template
        template = self.jinja_env.get_template("migration_guide.md")

        # Render guide
        content = template.render(
            from_version=from_version,
            to_version=to_version,
            migration_summary=migration_summary,
            examples=examples,
            generated_at=datetime.utcnow()
        )

        # Write to file
        output_path = self.output_dir / f"migration_{from_version}_to_{to_version}.md"
        output_path.write_text(content)

        logger.info(f"Generated migration guide: {output_path}")
        return output_path

    async def generate_compatibility_matrix(self) -> Path:
        """Generate compatibility matrix for all versions."""
        logger.info("Generating API version compatibility matrix")

        # Get version information
        versions = self.version_manager.versions

        # Build compatibility matrix
        compatibility_matrix = {}
        for from_version in versions.keys():
            compatibility_matrix[from_version] = {}
            for to_version in versions.keys():
                if from_version == to_version:
                    compatibility_matrix[from_version][to_version] = "identical"
                else:
                    from_info = versions[from_version]
                    to_info = versions[to_version]

                    if from_info.major != to_info.major:
                        compatibility_matrix[from_version][to_version] = "major_change"
                    elif from_info.minor != to_info.minor:
                        compatibility_matrix[from_version][to_version] = "minor_change"
                    else:
                        compatibility_matrix[from_version][to_version] = "patch_change"

        # Load template
        template = self.jinja_env.get_template("compatibility_matrix.md")

        # Render matrix
        content = template.render(
            versions=versions,
            compatibility_matrix=compatibility_matrix,
            compatibility_rules=self.version_manager.compatibility_rules,
            generated_at=datetime.utcnow()
        )

        # Write to file
        output_path = self.output_dir / "compatibility_matrix.md"
        output_path.write_text(content)

        logger.info(f"Generated compatibility matrix: {output_path}")
        return output_path

    async def generate_openapi_specs(self) -> Dict[str, Path]:
        """Generate OpenAPI specifications for each version."""
        logger.info("Generating OpenAPI specifications for all versions")

        specs = {}

        for version in self.version_manager.versions.keys():
            spec = await self._create_openapi_spec(version)

            # Write to file
            output_path = self.output_dir / f"openapi_v{version}.json"
            output_path.write_text(json.dumps(spec, indent=2))

            specs[version] = output_path
            logger.info(f"Generated OpenAPI spec for v{version}: {output_path}")

        return specs

    async def _create_openapi_spec(self, version: str) -> Dict[str, Any]:
        """Create OpenAPI specification for a specific version."""
        version_info = self.version_manager.versions[version]

        spec = {
            "openapi": "3.0.3",
            "info": {
                "title": f"PwC Data Platform API v{version}",
                "version": version,
                "description": f"API version {version} - {version_info.changelog}",
                "contact": {
                    "name": "API Support",
                    "email": "api-support@pwc.com"
                }
            },
            "servers": [
                {
                    "url": f"https://api.pwc-data-platform.com/v{version}",
                    "description": f"Production server for API v{version}"
                }
            ],
            "paths": {},
            "components": {
                "schemas": {},
                "securitySchemes": {
                    "bearerAuth": {
                        "type": "http",
                        "scheme": "bearer",
                        "bearerFormat": "JWT"
                    }
                }
            },
            "security": [{"bearerAuth": []}]
        }

        # Add version-specific paths and schemas
        if version == "1.0":
            spec["paths"] = {
                "/sales": {
                    "get": {
                        "summary": "Get sales data",
                        "parameters": [
                            {
                                "name": "date_from",
                                "in": "query",
                                "schema": {"type": "string", "format": "date"},
                                "description": "Start date (DD/MM/YYYY format)"
                            }
                        ],
                        "responses": {
                            "200": {
                                "description": "Sales data",
                                "content": {
                                    "application/json": {
                                        "schema": {"$ref": "#/components/schemas/SalesResponseV1"}
                                    }
                                }
                            }
                        }
                    }
                }
            }

            spec["components"]["schemas"]["SalesResponseV1"] = {
                "type": "object",
                "properties": {
                    "invoice_date": {"type": "string", "description": "Invoice date in DD/MM/YYYY format"},
                    "customer_country": {"type": "string", "description": "Customer country name"},
                    "total_amount": {"type": "number", "description": "Total amount"}
                }
            }

        elif version == "2.0":
            spec["paths"] = {
                "/sales": {
                    "get": {
                        "summary": "Get sales data",
                        "parameters": [
                            {
                                "name": "date_from",
                                "in": "query",
                                "schema": {"type": "string", "format": "date"},
                                "description": "Start date (ISO 8601 format)"
                            }
                        ],
                        "responses": {
                            "200": {
                                "description": "Sales data",
                                "content": {
                                    "application/json": {
                                        "schema": {"$ref": "#/components/schemas/SalesResponseV2"}
                                    }
                                }
                            }
                        }
                    }
                }
            }

            spec["components"]["schemas"]["SalesResponseV2"] = {
                "type": "object",
                "properties": {
                    "transaction_date": {"type": "string", "format": "date", "description": "Transaction date in ISO 8601 format"},
                    "country_code": {"type": "string", "description": "ISO country code"},
                    "total_amount": {"type": "number", "description": "Total amount"}
                }
            }

        # Add deprecation warnings if applicable
        if version_info.deprecation_level != DeprecationLevel.STABLE:
            spec["info"]["x-api-deprecated"] = True
            spec["info"]["x-deprecation-date"] = version_info.deprecation_date.isoformat() if version_info.deprecation_date else None
            spec["info"]["x-sunset-date"] = version_info.sunset_date.isoformat() if version_info.sunset_date else None

        return spec

    async def generate_changelog(self) -> Path:
        """Generate comprehensive changelog for all versions."""
        logger.info("Generating API changelog")

        # Sort versions by release date
        sorted_versions = sorted(
            self.version_manager.versions.items(),
            key=lambda x: x[1].release_date,
            reverse=True
        )

        changelog_content = ["# API Changelog\n"]

        for version, info in sorted_versions:
            changelog_content.append(f"## Version {version}")
            changelog_content.append(f"**Released:** {info.release_date.strftime('%Y-%m-%d')}")

            if info.deprecation_level != DeprecationLevel.STABLE:
                changelog_content.append(f"**Status:** {info.deprecation_level.value.title()}")

                if info.deprecation_date:
                    changelog_content.append(f"**Deprecated:** {info.deprecation_date.strftime('%Y-%m-%d')}")

                if info.sunset_date:
                    changelog_content.append(f"**Sunset:** {info.sunset_date.strftime('%Y-%m-%d')}")

            changelog_content.append(f"\n{info.changelog}\n")

            if info.breaking_changes:
                changelog_content.append("**Breaking Changes:**")
                for change in info.breaking_changes:
                    changelog_content.append(f"- {change}")
                changelog_content.append("")

            if info.migration_guide_url:
                changelog_content.append(f"**Migration Guide:** {info.migration_guide_url}\n")

            changelog_content.append("---\n")

        # Write to file
        output_path = self.output_dir / "CHANGELOG.md"
        output_path.write_text("\n".join(changelog_content))

        logger.info(f"Generated changelog: {output_path}")
        return output_path

    async def generate_all_documentation(self) -> Dict[str, Any]:
        """Generate all documentation artifacts."""
        logger.info("Generating complete API version documentation suite")

        results = {
            "version_docs": {},
            "migration_guides": {},
            "openapi_specs": {},
            "generated_at": datetime.utcnow().isoformat()
        }

        # Generate version documentation
        for version in self.version_manager.versions.keys():
            results["version_docs"][version] = str(await self.generate_version_documentation(version))

        # Generate migration guides
        versions = list(self.version_manager.versions.keys())
        for i, from_version in enumerate(versions):
            for to_version in versions[i+1:]:
                guide_path = await self.generate_migration_guide(from_version, to_version)
                results["migration_guides"][f"{from_version}_to_{to_version}"] = str(guide_path)

        # Generate OpenAPI specs
        results["openapi_specs"] = {
            version: str(path)
            for version, path in (await self.generate_openapi_specs()).items()
        }

        # Generate other artifacts
        results["compatibility_matrix"] = str(await self.generate_compatibility_matrix())
        results["changelog"] = str(await self.generate_changelog())

        # Generate index file
        await self._generate_index_file(results)

        logger.info("Complete documentation suite generated successfully")
        return results

    async def _generate_index_file(self, results: Dict[str, Any]):
        """Generate index file for documentation."""
        index_content = [
            "# API Version Documentation Index\n",
            f"Generated on: {results['generated_at']}\n",
            "## Version Documentation",
        ]

        for version, path in results["version_docs"].items():
            index_content.append(f"- [Version {version}]({Path(path).name})")

        index_content.extend([
            "\n## Migration Guides",
        ])

        for migration, path in results["migration_guides"].items():
            from_version, to_version = migration.split("_to_")
            index_content.append(f"- [Migration: {from_version} → {to_version}]({Path(path).name})")

        index_content.extend([
            "\n## OpenAPI Specifications",
        ])

        for version, path in results["openapi_specs"].items():
            index_content.append(f"- [OpenAPI v{version}]({Path(path).name})")

        index_content.extend([
            "\n## Other Documentation",
            f"- [Compatibility Matrix]({Path(results['compatibility_matrix']).name})",
            f"- [Changelog]({Path(results['changelog']).name})",
        ])

        # Write index file
        index_path = self.output_dir / "README.md"
        index_path.write_text("\n".join(index_content))

        logger.info(f"Generated documentation index: {index_path}")


# Factory function to create documentation generator
def create_documentation_generator(
    version_manager: APIVersionManager,
    migration_orchestrator: MigrationOrchestrator
) -> DocumentationGenerator:
    """Create documentation generator with enterprise configuration."""
    return DocumentationGenerator(version_manager, migration_orchestrator)