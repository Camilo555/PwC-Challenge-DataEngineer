#!/usr/bin/env python3
"""
Automated API Documentation Generator

This script automatically generates comprehensive API documentation from FastAPI OpenAPI specs:
- Extracts OpenAPI JSON schemas
- Generates markdown documentation
- Creates interactive HTML documentation
- Generates client SDKs examples
- Creates API testing guides
"""

import asyncio
import json
import sys
from pathlib import Path
from typing import Dict, Any, List, Optional
from datetime import datetime
from dataclasses import dataclass
from urllib.parse import urlparse

import httpx
import yaml
from jinja2 import Environment, FileSystemLoader, select_autoescape

# Add src directory to Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from api.main import app
from core.logging import get_logger

logger = get_logger(__name__)


@dataclass
class APIEndpoint:
    """API endpoint information."""
    path: str
    method: str
    summary: str
    description: str
    tags: List[str]
    parameters: List[Dict[str, Any]]
    request_schema: Optional[Dict[str, Any]]
    responses: Dict[str, Dict[str, Any]]
    security: List[Dict[str, Any]]
    deprecated: bool = False
    examples: List[Dict[str, Any]] = None


@dataclass
class APITag:
    """API tag information."""
    name: str
    description: str
    external_docs: Optional[Dict[str, str]] = None


@dataclass
class APIDocumentation:
    """Complete API documentation structure."""
    title: str
    version: str
    description: str
    base_url: str
    tags: List[APITag]
    endpoints: List[APIEndpoint]
    schemas: Dict[str, Any]
    security_definitions: Dict[str, Any]
    servers: List[Dict[str, Any]]
    contact: Optional[Dict[str, str]] = None
    license: Optional[Dict[str, str]] = None


class OpenAPIExtractor:
    """Extract OpenAPI specification from FastAPI app."""
    
    def __init__(self, fastapi_app):
        self.app = fastapi_app
    
    def extract_openapi_spec(self) -> Dict[str, Any]:
        """Extract OpenAPI specification from FastAPI app."""
        return self.app.openapi()
    
    def parse_openapi_spec(self, spec: Dict[str, Any]) -> APIDocumentation:
        """Parse OpenAPI spec into structured documentation."""
        info = spec.get("info", {})
        
        # Parse tags
        tags = []
        for tag_spec in spec.get("tags", []):
            tags.append(APITag(
                name=tag_spec["name"],
                description=tag_spec.get("description", ""),
                external_docs=tag_spec.get("externalDocs")
            ))
        
        # Parse endpoints
        endpoints = []
        for path, path_spec in spec.get("paths", {}).items():
            for method, method_spec in path_spec.items():
                if method in ["get", "post", "put", "delete", "patch", "options", "head"]:
                    endpoint = self._parse_endpoint(path, method, method_spec)
                    endpoints.append(endpoint)
        
        # Parse security definitions
        security_definitions = spec.get("components", {}).get("securitySchemes", {})
        
        return APIDocumentation(
            title=info.get("title", "API Documentation"),
            version=info.get("version", "1.0.0"),
            description=info.get("description", ""),
            base_url=self._extract_base_url(spec),
            tags=tags,
            endpoints=endpoints,
            schemas=spec.get("components", {}).get("schemas", {}),
            security_definitions=security_definitions,
            servers=spec.get("servers", [{"url": "/"}]),
            contact=info.get("contact"),
            license=info.get("license")
        )
    
    def _parse_endpoint(self, path: str, method: str, spec: Dict[str, Any]) -> APIEndpoint:
        """Parse individual endpoint specification."""
        parameters = []
        
        # Parse parameters
        for param in spec.get("parameters", []):
            parameters.append({
                "name": param["name"],
                "in": param["in"],
                "required": param.get("required", False),
                "type": param.get("schema", {}).get("type", "string"),
                "description": param.get("description", ""),
                "example": param.get("example")
            })
        
        # Parse request body
        request_schema = None
        request_body = spec.get("requestBody")
        if request_body:
            content = request_body.get("content", {})
            if "application/json" in content:
                request_schema = content["application/json"].get("schema")
        
        # Parse responses
        responses = {}
        for status_code, response_spec in spec.get("responses", {}).items():
            responses[status_code] = {
                "description": response_spec.get("description", ""),
                "schema": response_spec.get("content", {}).get("application/json", {}).get("schema"),
                "examples": response_spec.get("examples", {})
            }
        
        return APIEndpoint(
            path=path,
            method=method.upper(),
            summary=spec.get("summary", ""),
            description=spec.get("description", ""),
            tags=spec.get("tags", []),
            parameters=parameters,
            request_schema=request_schema,
            responses=responses,
            security=spec.get("security", []),
            deprecated=spec.get("deprecated", False),
            examples=self._extract_examples(spec)
        )
    
    def _extract_base_url(self, spec: Dict[str, Any]) -> str:
        """Extract base URL from OpenAPI spec."""
        servers = spec.get("servers", [])
        if servers:
            return servers[0]["url"]
        return "/"
    
    def _extract_examples(self, spec: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Extract examples from endpoint specification."""
        examples = []
        
        # Extract examples from request body
        request_body = spec.get("requestBody", {})
        if request_body:
            content = request_body.get("content", {})
            for media_type, media_spec in content.items():
                if "examples" in media_spec:
                    for name, example in media_spec["examples"].items():
                        examples.append({
                            "type": "request",
                            "media_type": media_type,
                            "name": name,
                            "value": example.get("value"),
                            "summary": example.get("summary", ""),
                            "description": example.get("description", "")
                        })
        
        return examples


class DocumentationGenerator:
    """Generate various formats of API documentation."""
    
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Setup Jinja2 template environment
        template_dir = Path(__file__).parent / "templates" / "api_docs"
        self.jinja_env = Environment(
            loader=FileSystemLoader(template_dir),
            autoescape=select_autoescape(['html', 'xml'])
        )
    
    def generate_markdown_docs(self, api_doc: APIDocumentation) -> Path:
        """Generate comprehensive Markdown documentation."""
        md_content = self._generate_markdown_content(api_doc)
        
        output_file = self.output_dir / "API_COMPREHENSIVE_DOCUMENTATION.md"
        output_file.write_text(md_content, encoding='utf-8')
        
        logger.info(f"Generated Markdown documentation: {output_file}")
        return output_file
    
    def generate_html_docs(self, api_doc: APIDocumentation) -> Path:
        """Generate interactive HTML documentation."""
        try:
            template = self.jinja_env.get_template("api_docs.html")
            html_content = template.render(api=api_doc)
        except Exception:
            # Fallback to simple HTML generation
            html_content = self._generate_fallback_html(api_doc)
        
        output_file = self.output_dir / "API_INTERACTIVE_DOCUMENTATION.html"
        output_file.write_text(html_content, encoding='utf-8')
        
        logger.info(f"Generated HTML documentation: {output_file}")
        return output_file
    
    def generate_postman_collection(self, api_doc: APIDocumentation) -> Path:
        """Generate Postman collection for API testing."""
        collection = self._create_postman_collection(api_doc)
        
        output_file = self.output_dir / "API_POSTMAN_COLLECTION.json"
        output_file.write_text(json.dumps(collection, indent=2), encoding='utf-8')
        
        logger.info(f"Generated Postman collection: {output_file}")
        return output_file
    
    def generate_curl_examples(self, api_doc: APIDocumentation) -> Path:
        """Generate cURL examples for all endpoints."""
        curl_content = self._generate_curl_examples(api_doc)
        
        output_file = self.output_dir / "API_CURL_EXAMPLES.md"
        output_file.write_text(curl_content, encoding='utf-8')
        
        logger.info(f"Generated cURL examples: {output_file}")
        return output_file
    
    def generate_python_sdk_examples(self, api_doc: APIDocumentation) -> Path:
        """Generate Python SDK usage examples."""
        python_content = self._generate_python_examples(api_doc)
        
        output_file = self.output_dir / "API_PYTHON_SDK_EXAMPLES.py"
        output_file.write_text(python_content, encoding='utf-8')
        
        logger.info(f"Generated Python SDK examples: {output_file}")
        return output_file
    
    def generate_openapi_yaml(self, spec: Dict[str, Any]) -> Path:
        """Generate OpenAPI YAML specification."""
        output_file = self.output_dir / "openapi.yaml"
        output_file.write_text(yaml.dump(spec, default_flow_style=False), encoding='utf-8')
        
        logger.info(f"Generated OpenAPI YAML: {output_file}")
        return output_file
    
    def _generate_markdown_content(self, api_doc: APIDocumentation) -> str:
        """Generate comprehensive Markdown documentation content."""
        content = f"""# {api_doc.title}

**Version:** {api_doc.version}

{api_doc.description}

---

## Table of Contents

1. [Overview](#overview)
2. [Authentication](#authentication)
3. [Base URLs](#base-urls)
4. [API Endpoints](#api-endpoints)
5. [Data Models](#data-models)
6. [Error Handling](#error-handling)
7. [Rate Limiting](#rate-limiting)
8. [Examples](#examples)

---

## Overview

This API provides comprehensive endpoints for the PwC Data Engineering Challenge platform, featuring:

- **Enterprise Security**: Advanced authentication, authorization, and data protection
- **High Performance**: Optimized for high-throughput data processing
- **Scalability**: Microservices architecture with service discovery
- **Compliance**: GDPR, HIPAA, PCI-DSS, SOX compliance
- **Real-time Features**: WebSocket support for live updates

### API Features

- RESTful design with OpenAPI 3.0 specification
- JSON-based request/response format with ORJSON optimization
- Comprehensive error handling with detailed error messages
- Rate limiting and throttling for API protection
- Real-time WebSocket connections for live data
- GraphQL endpoint for flexible data querying

---

## Authentication

The API supports multiple authentication methods:

### JWT Bearer Tokens (Recommended)

```http
Authorization: Bearer <jwt_token>
```

### API Keys

```http
Authorization: Bearer pwc_<api_key>
```

### OAuth2/OIDC

Supports OAuth2 authorization code flow with PKCE.

### Basic Authentication (Fallback)

```http
Authorization: Basic <base64(username:password)>
```

---

## Base URLs

"""

        # Add server information
        for server in api_doc.servers:
            content += f"- `{server['url']}`"
            if 'description' in server:
                content += f" - {server['description']}"
            content += "\n"

        content += "\n---\n\n## API Endpoints\n\n"

        # Group endpoints by tags
        endpoints_by_tag = {}
        for endpoint in api_doc.endpoints:
            for tag in endpoint.tags:
                if tag not in endpoints_by_tag:
                    endpoints_by_tag[tag] = []
                endpoints_by_tag[tag].append(endpoint)
        
        # Generate documentation for each tag group
        for tag in api_doc.tags:
            content += f"### {tag.name}\n\n"
            if tag.description:
                content += f"{tag.description}\n\n"
            
            tag_endpoints = endpoints_by_tag.get(tag.name, [])
            for endpoint in tag_endpoints:
                content += self._format_endpoint_markdown(endpoint)
            
            content += "\n"

        # Add data models section
        content += self._generate_schemas_markdown(api_doc.schemas)
        
        # Add error handling section
        content += self._generate_error_handling_section()
        
        # Add examples section
        content += self._generate_examples_section(api_doc)

        return content
    
    def _format_endpoint_markdown(self, endpoint: APIEndpoint) -> str:
        """Format individual endpoint as Markdown."""
        method_badge = {
            "GET": "🟢 GET",
            "POST": "🔵 POST", 
            "PUT": "🟡 PUT",
            "DELETE": "🔴 DELETE",
            "PATCH": "🟠 PATCH"
        }.get(endpoint.method, f"⚪ {endpoint.method}")
        
        content = f"#### {method_badge} `{endpoint.path}`\n\n"
        
        if endpoint.summary:
            content += f"**Summary:** {endpoint.summary}\n\n"
        
        if endpoint.description:
            content += f"{endpoint.description}\n\n"
        
        if endpoint.deprecated:
            content += "⚠️ **DEPRECATED** - This endpoint is deprecated and may be removed in future versions.\n\n"
        
        # Parameters
        if endpoint.parameters:
            content += "**Parameters:**\n\n"
            content += "| Name | Type | In | Required | Description |\n"
            content += "|------|------|----|---------|--------------|\n"
            
            for param in endpoint.parameters:
                required = "✅" if param["required"] else "❌"
                description = param["description"] or "No description"
                content += f"| `{param['name']}` | {param['type']} | {param['in']} | {required} | {description} |\n"
            
            content += "\n"
        
        # Request body
        if endpoint.request_schema:
            content += "**Request Body:**\n\n"
            content += "```json\n"
            content += json.dumps(self._schema_to_example(endpoint.request_schema), indent=2)
            content += "\n```\n\n"
        
        # Responses
        if endpoint.responses:
            content += "**Responses:**\n\n"
            for status_code, response in endpoint.responses.items():
                content += f"**{status_code}** - {response['description']}\n\n"
                if response.get("schema"):
                    content += "```json\n"
                    content += json.dumps(self._schema_to_example(response["schema"]), indent=2)
                    content += "\n```\n\n"
        
        # Security
        if endpoint.security:
            content += "**Security:** Requires authentication\n\n"
        
        content += "---\n\n"
        return content
    
    def _schema_to_example(self, schema: Dict[str, Any]) -> Any:
        """Convert JSON schema to example value."""
        if not schema:
            return {}
        
        schema_type = schema.get("type", "object")
        
        if schema_type == "object":
            properties = schema.get("properties", {})
            example = {}
            for prop_name, prop_schema in properties.items():
                example[prop_name] = self._schema_to_example(prop_schema)
            return example
        
        elif schema_type == "array":
            items_schema = schema.get("items", {})
            return [self._schema_to_example(items_schema)]
        
        elif schema_type == "string":
            return schema.get("example", "string")
        
        elif schema_type == "integer":
            return schema.get("example", 42)
        
        elif schema_type == "number":
            return schema.get("example", 3.14)
        
        elif schema_type == "boolean":
            return schema.get("example", True)
        
        else:
            return schema.get("example", f"<{schema_type}>")
    
    def _generate_schemas_markdown(self, schemas: Dict[str, Any]) -> str:
        """Generate data models documentation."""
        if not schemas:
            return ""
        
        content = "## Data Models\n\n"
        
        for schema_name, schema in schemas.items():
            content += f"### {schema_name}\n\n"
            
            if schema.get("description"):
                content += f"{schema['description']}\n\n"
            
            properties = schema.get("properties", {})
            if properties:
                content += "**Properties:**\n\n"
                content += "| Field | Type | Required | Description |\n"
                content += "|-------|------|----------|-------------|\n"
                
                required_fields = schema.get("required", [])
                
                for prop_name, prop_schema in properties.items():
                    prop_type = prop_schema.get("type", "unknown")
                    is_required = "✅" if prop_name in required_fields else "❌"
                    description = prop_schema.get("description", "No description")
                    
                    content += f"| `{prop_name}` | {prop_type} | {is_required} | {description} |\n"
                
                content += "\n"
            
            # Example
            content += "**Example:**\n\n"
            content += "```json\n"
            content += json.dumps(self._schema_to_example(schema), indent=2)
            content += "\n```\n\n"
        
        return content
    
    def _generate_error_handling_section(self) -> str:
        """Generate error handling documentation."""
        return """## Error Handling

The API uses standard HTTP status codes and returns detailed error messages in JSON format.

### Common Status Codes

| Code | Status | Description |
|------|--------|-------------|
| 200 | OK | Request successful |
| 201 | Created | Resource created successfully |
| 400 | Bad Request | Invalid request parameters |
| 401 | Unauthorized | Authentication required |
| 403 | Forbidden | Insufficient permissions |
| 404 | Not Found | Resource not found |
| 422 | Unprocessable Entity | Validation error |
| 429 | Too Many Requests | Rate limit exceeded |
| 500 | Internal Server Error | Server error |

### Error Response Format

```json
{
  "detail": "Error message",
  "type": "error_type",
  "code": "ERROR_CODE",
  "timestamp": "2024-01-01T12:00:00Z",
  "request_id": "uuid-string"
}
```

---

## Rate Limiting

The API implements rate limiting to prevent abuse:

- **Default Limit:** 100 requests per minute
- **Authentication Endpoints:** 5 requests per minute  
- **Analytics Endpoints:** 20 requests per minute with burst to 30
- **Headers:** Rate limit information is returned in response headers:
  - `X-RateLimit-Limit`: Maximum requests per window
  - `X-RateLimit-Remaining`: Remaining requests in current window
  - `X-RateLimit-Reset`: Time when the rate limit resets

---

"""
    
    def _generate_examples_section(self, api_doc: APIDocumentation) -> str:
        """Generate examples section."""
        content = "## Examples\n\n"
        
        # Add common authentication example
        content += "### Authentication Example\n\n"
        content += "```bash\n"
        content += 'curl -X GET "https://api.example.com/api/v1/health" \\\n'
        content += '  -H "Authorization: Bearer YOUR_JWT_TOKEN"\n'
        content += "```\n\n"
        
        # Add a few endpoint examples
        content += "### Common Operations\n\n"
        
        example_endpoints = [ep for ep in api_doc.endpoints if ep.method == "GET"][:3]
        for endpoint in example_endpoints:
            content += f"#### {endpoint.summary or endpoint.path}\n\n"
            content += "```bash\n"
            content += f'curl -X {endpoint.method} "https://api.example.com{endpoint.path}" \\\n'
            content += '  -H "Authorization: Bearer YOUR_JWT_TOKEN" \\\n'
            content += '  -H "Content-Type: application/json"\n'
            content += "```\n\n"
        
        return content
    
    def _generate_fallback_html(self, api_doc: APIDocumentation) -> str:
        """Generate fallback HTML documentation."""
        return f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>{api_doc.title} - API Documentation</title>
    <style>
        body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; line-height: 1.6; margin: 2rem; }}
        .header {{ background: #f8f9fa; padding: 2rem; border-radius: 8px; margin-bottom: 2rem; }}
        .endpoint {{ border: 1px solid #dee2e6; border-radius: 8px; margin: 1rem 0; padding: 1rem; }}
        .method-get {{ border-left: 4px solid #28a745; }}
        .method-post {{ border-left: 4px solid #007bff; }}
        .method-put {{ border-left: 4px solid #ffc107; }}
        .method-delete {{ border-left: 4px solid #dc3545; }}
        .method {{ font-weight: bold; text-transform: uppercase; }}
        .path {{ font-family: monospace; background: #f8f9fa; padding: 0.2rem 0.5rem; border-radius: 4px; }}
        pre {{ background: #f8f9fa; padding: 1rem; border-radius: 4px; overflow-x: auto; }}
        .deprecated {{ background: #fff3cd; border-color: #ffeaa7; }}
    </style>
</head>
<body>
    <div class="header">
        <h1>{api_doc.title}</h1>
        <p><strong>Version:</strong> {api_doc.version}</p>
        <p>{api_doc.description}</p>
    </div>

    <h2>Endpoints</h2>
"""

        # Add endpoints
        for endpoint in api_doc.endpoints:
            method_class = f"method-{endpoint.method.lower()}"
            deprecated_class = " deprecated" if endpoint.deprecated else ""
            
            html_content = f"""
    <div class="endpoint {method_class}{deprecated_class}">
        <h3><span class="method">{endpoint.method}</span> <span class="path">{endpoint.path}</span></h3>
        <p><strong>Summary:</strong> {endpoint.summary}</p>
        <p>{endpoint.description}</p>
        
        <h4>Parameters</h4>
        <ul>
"""
            
            for param in endpoint.parameters:
                required = " (required)" if param["required"] else ""
                html_content += f"""<li><code>{param['name']}</code> ({param['type']}) - {param['description']}{required}</li>"""
            
            html_content += """
        </ul>
    </div>
"""
        
        html_content += """
</body>
</html>"""
        
        return html_content
    
    def _create_postman_collection(self, api_doc: APIDocumentation) -> Dict[str, Any]:
        """Create Postman collection structure."""
        collection = {
            "info": {
                "name": api_doc.title,
                "description": api_doc.description,
                "version": api_doc.version,
                "schema": "https://schema.getpostman.com/json/collection/v2.1.0/collection.json"
            },
            "auth": {
                "type": "bearer",
                "bearer": [
                    {
                        "key": "token",
                        "value": "{{jwt_token}}",
                        "type": "string"
                    }
                ]
            },
            "variable": [
                {
                    "key": "base_url",
                    "value": "https://api.example.com",
                    "type": "string"
                },
                {
                    "key": "jwt_token",
                    "value": "your_jwt_token_here",
                    "type": "string"
                }
            ],
            "item": []
        }
        
        # Group items by tags
        for tag in api_doc.tags:
            tag_item = {
                "name": tag.name,
                "description": tag.description,
                "item": []
            }
            
            for endpoint in api_doc.endpoints:
                if tag.name in endpoint.tags:
                    request_item = {
                        "name": endpoint.summary or f"{endpoint.method} {endpoint.path}",
                        "request": {
                            "method": endpoint.method,
                            "header": [
                                {
                                    "key": "Content-Type",
                                    "value": "application/json"
                                }
                            ],
                            "url": {
                                "raw": "{{base_url}}" + endpoint.path,
                                "host": ["{{base_url}}"],
                                "path": endpoint.path.strip("/").split("/")
                            },
                            "description": endpoint.description
                        }
                    }
                    
                    # Add request body if present
                    if endpoint.request_schema:
                        request_item["request"]["body"] = {
                            "mode": "raw",
                            "raw": json.dumps(self._schema_to_example(endpoint.request_schema), indent=2)
                        }
                    
                    tag_item["item"].append(request_item)
            
            if tag_item["item"]:
                collection["item"].append(tag_item)
        
        return collection
    
    def _generate_curl_examples(self, api_doc: APIDocumentation) -> str:
        """Generate comprehensive cURL examples."""
        content = f"# {api_doc.title} - cURL Examples\n\n"
        content += f"Version: {api_doc.version}\n\n"
        content += "This document contains cURL examples for all API endpoints.\n\n"
        content += "## Setup\n\n"
        content += "Set your base URL and JWT token:\n\n"
        content += "```bash\n"
        content += 'export API_BASE_URL="https://api.example.com"\n'
        content += 'export JWT_TOKEN="your_jwt_token_here"\n'
        content += "```\n\n"
        
        for tag in api_doc.tags:
            content += f"## {tag.name}\n\n"
            if tag.description:
                content += f"{tag.description}\n\n"
            
            tag_endpoints = [ep for ep in api_doc.endpoints if tag.name in ep.tags]
            
            for endpoint in tag_endpoints:
                content += f"### {endpoint.summary or endpoint.path}\n\n"
                
                if endpoint.description:
                    content += f"{endpoint.description}\n\n"
                
                # Build cURL command
                curl_cmd = f'curl -X {endpoint.method} "$API_BASE_URL{endpoint.path}"'
                
                # Add headers
                curl_cmd += ' \\\n  -H "Authorization: Bearer $JWT_TOKEN"'
                curl_cmd += ' \\\n  -H "Content-Type: application/json"'
                
                # Add request body if present
                if endpoint.request_schema:
                    example_body = json.dumps(self._schema_to_example(endpoint.request_schema), indent=2)
                    curl_cmd += f" \\\n  -d '{example_body}'"
                
                content += "```bash\n"
                content += curl_cmd + "\n"
                content += "```\n\n"
        
        return content
    
    def _generate_python_examples(self, api_doc: APIDocumentation) -> str:
        """Generate Python SDK usage examples."""
        return f'''"""
{api_doc.title} - Python SDK Examples

Version: {api_doc.version}

This module contains Python examples for using the API with popular HTTP libraries.
"""

import httpx
import asyncio
from typing import Dict, Any, Optional


class APIClient:
    """Simple API client for {api_doc.title}."""
    
    def __init__(self, base_url: str, token: str):
        self.base_url = base_url.rstrip("/")
        self.headers = {{
            "Authorization": f"Bearer {{token}}",
            "Content-Type": "application/json"
        }}
    
    async def get(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Make GET request."""
        async with httpx.AsyncClient() as client:
            response = await client.get(
                f"{{self.base_url}}{{path}}",
                headers=self.headers,
                params=params or {{}}
            )
            response.raise_for_status()
            return response.json()
    
    async def post(self, path: str, data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Make POST request."""
        async with httpx.AsyncClient() as client:
            response = await client.post(
                f"{{self.base_url}}{{path}}",
                headers=self.headers,
                json=data or {{}}
            )
            response.raise_for_status()
            return response.json()


# Example usage
async def main():
    """Example usage of the API client."""
    client = APIClient("https://api.example.com", "your_jwt_token_here")
    
    # Health check
    health = await client.get("/api/v1/health")
    print("Health check:", health)
    
    # Get sales summary
    sales_summary = await client.get("/api/v1/sales/summary")
    print("Sales summary:", sales_summary)


# Synchronous examples using requests
import requests

def sync_examples():
    """Synchronous API examples using requests."""
    base_url = "https://api.example.com"
    headers = {{
        "Authorization": "Bearer your_jwt_token_here",
        "Content-Type": "application/json"
    }}
    
    # Health check
    response = requests.get(f"{{base_url}}/api/v1/health", headers=headers)
    print("Health:", response.json())
    
    # Sales data
    response = requests.get(f"{{base_url}}/api/v1/sales/summary", headers=headers)
    print("Sales:", response.json())


if __name__ == "__main__":
    # Run async examples
    asyncio.run(main())
    
    # Run sync examples
    sync_examples()
'''


async def main():
    """Main function to generate API documentation."""
    logger.info("Starting API documentation generation")
    
    try:
        # Extract OpenAPI specification
        extractor = OpenAPIExtractor(app)
        openapi_spec = extractor.extract_openapi_spec()
        api_doc = extractor.parse_openapi_spec(openapi_spec)
        
        logger.info(f"Extracted API specification: {len(api_doc.endpoints)} endpoints, {len(api_doc.tags)} tags")
        
        # Setup output directory
        docs_dir = Path(__file__).parent.parent / "docs" / "api"
        docs_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate documentation
        generator = DocumentationGenerator(docs_dir)
        
        generated_files = []
        
        # Generate different formats
        generated_files.append(generator.generate_markdown_docs(api_doc))
        generated_files.append(generator.generate_html_docs(api_doc))
        generated_files.append(generator.generate_postman_collection(api_doc))
        generated_files.append(generator.generate_curl_examples(api_doc))
        generated_files.append(generator.generate_python_sdk_examples(api_doc))
        generated_files.append(generator.generate_openapi_yaml(openapi_spec))
        
        # Generate summary report
        summary_content = f"""# API Documentation Generation Report

**Generated on:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## API Overview

- **Title:** {api_doc.title}
- **Version:** {api_doc.version}
- **Total Endpoints:** {len(api_doc.endpoints)}
- **API Tags:** {len(api_doc.tags)}
- **Data Models:** {len(api_doc.schemas)}

## Generated Files

"""
        
        for file_path in generated_files:
            file_size = file_path.stat().st_size
            summary_content += f"- **{file_path.name}** ({file_size:,} bytes)\n"
        
        summary_content += f"""

## Endpoints by Method

"""
        
        # Count endpoints by method
        method_counts = {}
        for endpoint in api_doc.endpoints:
            method_counts[endpoint.method] = method_counts.get(endpoint.method, 0) + 1
        
        for method, count in sorted(method_counts.items()):
            summary_content += f"- **{method}:** {count} endpoints\n"
        
        summary_content += f"""

## Endpoints by Tag

"""
        
        # Count endpoints by tag
        tag_counts = {}
        for endpoint in api_doc.endpoints:
            for tag in endpoint.tags:
                tag_counts[tag] = tag_counts.get(tag, 0) + 1
        
        for tag, count in sorted(tag_counts.items()):
            summary_content += f"- **{tag}:** {count} endpoints\n"
        
        # Security features
        summary_content += f"""

## Security Features

- **Authentication Methods:** JWT, API Keys, OAuth2/OIDC, Basic Auth
- **Authorization:** RBAC/ABAC with role-based permissions
- **Data Protection:** DLP with PII/PHI detection and redaction
- **Compliance:** GDPR, HIPAA, PCI-DSS, SOX
- **Rate Limiting:** Configurable per-endpoint limits
- **Security Headers:** CORS, CSP, HSTS, etc.

## Usage

The generated documentation includes:

1. **Comprehensive Markdown** - Complete API reference
2. **Interactive HTML** - Browser-viewable documentation  
3. **Postman Collection** - Import for API testing
4. **cURL Examples** - Command-line usage examples
5. **Python SDK Examples** - Programming language examples
6. **OpenAPI YAML** - Machine-readable specification

## Next Steps

1. Review the generated documentation for accuracy
2. Import the Postman collection for testing
3. Use the cURL examples to verify endpoints
4. Integrate the OpenAPI spec with API management tools
5. Share the HTML documentation with stakeholders
"""
        
        summary_file = docs_dir / "API_GENERATION_REPORT.md"
        summary_file.write_text(summary_content, encoding='utf-8')
        
        logger.info("API documentation generation completed successfully!")
        logger.info(f"Generated {len(generated_files)} documentation files")
        logger.info(f"Output directory: {docs_dir}")
        
        # Print summary
        print("\n" + "="*60)
        print("API DOCUMENTATION GENERATION COMPLETE")
        print("="*60)
        print(f"API: {api_doc.title} v{api_doc.version}")
        print(f"Endpoints: {len(api_doc.endpoints)}")
        print(f"Tags: {len(api_doc.tags)}")
        print(f"Output: {docs_dir}")
        print("\nGenerated files:")
        for file_path in generated_files + [summary_file]:
            print(f"  - {file_path.name}")
        print("="*60)
        
    except Exception as e:
        logger.error(f"API documentation generation failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(main())