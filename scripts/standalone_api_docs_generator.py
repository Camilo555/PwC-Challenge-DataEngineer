#!/usr/bin/env python3
"""
Standalone API Documentation Generator

Generates comprehensive API documentation without requiring the full application to run.
Extracts information from route files and schemas to build documentation.
"""

import json
import re
import sys
from pathlib import Path
from typing import Dict, Any, List, Set
from datetime import datetime
from dataclasses import dataclass, field
import ast
import inspect

# Add src directory to Python path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))


@dataclass
class EndpointInfo:
    """Information about an API endpoint."""
    path: str
    method: str
    function_name: str
    summary: str = ""
    description: str = ""
    tags: List[str] = field(default_factory=list)
    parameters: List[Dict[str, Any]] = field(default_factory=list)
    request_body: Dict[str, Any] = field(default_factory=dict)
    responses: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    security_required: bool = True
    deprecated: bool = False


@dataclass
class APIDocumentation:
    """Complete API documentation structure."""
    title: str = "PwC Data Engineering Challenge - Enterprise API"
    version: str = "4.0.0"
    description: str = "Enterprise-grade high-performance REST API"
    endpoints: List[EndpointInfo] = field(default_factory=list)
    tags: Set[str] = field(default_factory=set)
    schemas: Dict[str, Any] = field(default_factory=dict)


class RouteAnalyzer:
    """Analyze FastAPI route files to extract endpoint information."""
    
    def __init__(self, src_dir: Path):
        self.src_dir = src_dir
        self.api_dir = src_dir / "api"
    
    def analyze_routes(self) -> APIDocumentation:
        """Analyze all route files and extract API documentation."""
        doc = APIDocumentation()
        
        # Find all router files
        route_files = []
        for version_dir in ["v1", "v2"]:
            routes_dir = self.api_dir / version_dir / "routes"
            if routes_dir.exists():
                route_files.extend(routes_dir.glob("*.py"))
        
        # Analyze each route file
        for route_file in route_files:
            if route_file.name.startswith("__"):
                continue
            
            endpoints = self._analyze_route_file(route_file)
            doc.endpoints.extend(endpoints)
            
            # Extract tags
            for endpoint in endpoints:
                doc.tags.update(endpoint.tags)
        
        # Analyze schemas
        doc.schemas = self._analyze_schemas()
        
        return doc
    
    def _analyze_route_file(self, file_path: Path) -> List[EndpointInfo]:
        """Analyze a single route file."""
        endpoints = []
        
        try:
            content = file_path.read_text(encoding='utf-8')
            tree = ast.parse(content)
            
            for node in ast.walk(tree):
                if isinstance(node, ast.FunctionDef):
                    endpoint_info = self._extract_endpoint_info(node, content, file_path)
                    if endpoint_info:
                        endpoints.append(endpoint_info)
        
        except Exception as e:
            print(f"Error analyzing {file_path}: {e}")
        
        return endpoints
    
    def _extract_endpoint_info(self, func_node: ast.FunctionDef, content: str, file_path: Path) -> EndpointInfo:
        """Extract endpoint information from function node."""
        # Look for route decorators
        route_info = None
        
        for decorator in func_node.decorator_list:
            if isinstance(decorator, ast.Call):
                if hasattr(decorator.func, 'attr'):
                    method = decorator.func.attr.lower()
                    if method in ['get', 'post', 'put', 'delete', 'patch']:
                        # Extract path from decorator arguments
                        path = "/"
                        if decorator.args and isinstance(decorator.args[0], ast.Constant):
                            path = decorator.args[0].value
                        
                        route_info = (method.upper(), path)
                        break
        
        if not route_info:
            return None
        
        method, path = route_info
        
        # Extract docstring
        docstring = ast.get_docstring(func_node) or ""
        
        # Parse docstring for summary and description
        lines = docstring.split('\n')
        summary = lines[0].strip() if lines else ""
        description = '\n'.join(lines[1:]).strip() if len(lines) > 1 else ""
        
        # Determine tags based on file path
        tags = self._determine_tags(file_path, func_node.name)
        
        # Extract parameters (simplified)
        parameters = self._extract_parameters(func_node)
        
        # Determine version prefix
        version_prefix = "/api/v2" if "v2" in str(file_path) else "/api/v1"
        full_path = version_prefix + path
        
        return EndpointInfo(
            path=full_path,
            method=method,
            function_name=func_node.name,
            summary=summary or f"{method} {full_path}",
            description=description,
            tags=tags,
            parameters=parameters,
            security_required=self._requires_auth(func_node, content)
        )
    
    def _determine_tags(self, file_path: Path, function_name: str) -> List[str]:
        """Determine tags based on file path and function name."""
        file_name = file_path.stem
        
        tag_mapping = {
            'auth': ['Authentication'],
            'sales': ['Sales'],
            'analytics': ['Analytics'],
            'security': ['Security'],
            'health': ['Health'],
            'monitoring': ['Monitoring'],
            'enterprise': ['Enterprise'],
            'datamart': ['Data Management'],
            'features': ['Features'],
            'search': ['Search'],
            'supabase': ['Database'],
            'async_tasks': ['Background Tasks']
        }
        
        return tag_mapping.get(file_name, ['API'])
    
    def _extract_parameters(self, func_node: ast.FunctionDef) -> List[Dict[str, Any]]:
        """Extract parameter information from function signature."""
        parameters = []
        
        for arg in func_node.args.args:
            if arg.arg in ['current_user', 'credentials', 'token']:
                continue  # Skip auth-related parameters
            
            param_info = {
                "name": arg.arg,
                "type": "string",
                "required": True,
                "description": f"Parameter {arg.arg}"
            }
            
            # Try to infer type from annotation
            if arg.annotation:
                if hasattr(arg.annotation, 'id'):
                    type_name = arg.annotation.id
                    if type_name == 'int':
                        param_info["type"] = "integer"
                    elif type_name == 'float':
                        param_info["type"] = "number"
                    elif type_name == 'bool':
                        param_info["type"] = "boolean"
            
            parameters.append(param_info)
        
        return parameters
    
    def _requires_auth(self, func_node: ast.FunctionDef, content: str) -> bool:
        """Check if endpoint requires authentication."""
        # Look for dependency injection indicating auth requirement
        for arg in func_node.args.args:
            if arg.arg in ['current_user', 'credentials', 'token']:
                return True
        
        # Check for Depends in function signature
        if 'Depends(' in content:
            func_start = content.find(f"def {func_node.name}")
            if func_start > -1:
                func_end = content.find('\n\n', func_start)
                func_signature = content[func_start:func_end]
                if 'auth_dependency' in func_signature or 'verify_' in func_signature:
                    return True
        
        return False
    
    def _analyze_schemas(self) -> Dict[str, Any]:
        """Analyze schema files to extract data models."""
        schemas = {}
        
        # Look for schema files
        schema_dirs = [
            self.api_dir / "v1" / "schemas",
            self.api_dir / "v2" / "schemas",
            self.src_dir / "domain" / "entities"
        ]
        
        for schema_dir in schema_dirs:
            if schema_dir.exists():
                for schema_file in schema_dir.glob("*.py"):
                    if schema_file.name.startswith("__"):
                        continue
                    
                    file_schemas = self._extract_schemas_from_file(schema_file)
                    schemas.update(file_schemas)
        
        return schemas
    
    def _extract_schemas_from_file(self, file_path: Path) -> Dict[str, Any]:
        """Extract Pydantic schemas from a Python file."""
        schemas = {}
        
        try:
            content = file_path.read_text(encoding='utf-8')
            
            # Look for Pydantic model definitions
            class_pattern = r'class\s+(\w+)\s*\([^)]*BaseModel[^)]*\):'
            matches = re.finditer(class_pattern, content)
            
            for match in matches:
                class_name = match.group(1)
                schemas[class_name] = {
                    "type": "object",
                    "description": f"Schema for {class_name}",
                    "properties": self._extract_fields_from_class(content, class_name)
                }
        
        except Exception as e:
            print(f"Error extracting schemas from {file_path}: {e}")
        
        return schemas
    
    def _extract_fields_from_class(self, content: str, class_name: str) -> Dict[str, Any]:
        """Extract field definitions from Pydantic class."""
        properties = {}
        
        # Simple field extraction using regex
        class_start = content.find(f"class {class_name}")
        if class_start == -1:
            return properties
        
        # Find class body
        class_body_start = content.find(':', class_start) + 1
        next_class = content.find('\nclass ', class_body_start)
        class_body_end = next_class if next_class > -1 else len(content)
        
        class_body = content[class_body_start:class_body_end]
        
        # Extract field definitions
        field_pattern = r'^\s*(\w+)\s*:\s*([^\n]+)'
        for match in re.finditer(field_pattern, class_body, re.MULTILINE):
            field_name = match.group(1)
            field_type = match.group(2).strip()
            
            if field_name.startswith('_') or field_name in ['Config', 'Meta']:
                continue
            
            # Map Python types to JSON schema types
            json_type = self._map_python_type_to_json(field_type)
            
            properties[field_name] = {
                "type": json_type,
                "description": f"Field {field_name}"
            }
        
        return properties
    
    def _map_python_type_to_json(self, python_type: str) -> str:
        """Map Python type annotations to JSON schema types."""
        python_type = python_type.lower()
        
        if 'str' in python_type:
            return "string"
        elif 'int' in python_type:
            return "integer"
        elif 'float' in python_type:
            return "number"
        elif 'bool' in python_type:
            return "boolean"
        elif 'list' in python_type:
            return "array"
        elif 'dict' in python_type:
            return "object"
        else:
            return "string"


class DocumentationRenderer:
    """Render API documentation in various formats."""
    
    def __init__(self, output_dir: Path):
        self.output_dir = output_dir
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def render_markdown(self, doc: APIDocumentation) -> Path:
        """Render comprehensive Markdown documentation."""
        content = f"""# {doc.title}

**Version:** {doc.version}  
**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

{doc.description}

---

## Overview

This API provides comprehensive endpoints for the PwC Data Engineering Challenge platform, featuring:

- **Enterprise Security**: Advanced authentication, authorization, and data protection
- **High Performance**: Optimized for high-throughput data processing  
- **Scalability**: Microservices architecture with service discovery
- **Compliance**: GDPR, HIPAA, PCI-DSS, SOX compliance
- **Real-time Features**: WebSocket support for live updates

### Key Features

- RESTful design following OpenAPI 3.0 specification
- JSON-based request/response format with performance optimization
- Comprehensive authentication (JWT, API Keys, OAuth2/OIDC)
- Role-based access control (RBAC) with fine-grained permissions
- Rate limiting and throttling for API protection
- Real-time WebSocket connections for live data updates
- GraphQL endpoint for flexible data querying
- Comprehensive error handling with detailed messages

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

---

## Base URL

- Production: `https://api.pwc-challenge.com`
- Staging: `https://staging-api.pwc-challenge.com`  
- Development: `http://localhost:8000`

---

## API Endpoints

Total Endpoints: **{len(doc.endpoints)}**

"""

        # Group endpoints by tags
        endpoints_by_tag = {}
        for endpoint in doc.endpoints:
            for tag in endpoint.tags:
                if tag not in endpoints_by_tag:
                    endpoints_by_tag[tag] = []
                endpoints_by_tag[tag].append(endpoint)
        
        # Render each tag section
        for tag in sorted(doc.tags):
            if tag in endpoints_by_tag:
                content += f"### {tag}\n\n"
                
                for endpoint in endpoints_by_tag[tag]:
                    content += self._render_endpoint_markdown(endpoint)
                
                content += "\n"
        
        # Add schemas section
        if doc.schemas:
            content += "## Data Models\n\n"
            for schema_name, schema in doc.schemas.items():
                content += f"### {schema_name}\n\n"
                content += f"{schema.get('description', 'No description available')}\n\n"
                
                properties = schema.get('properties', {})
                if properties:
                    content += "**Properties:**\n\n"
                    content += "| Field | Type | Description |\n"
                    content += "|-------|------|--------------|\n"
                    
                    for prop_name, prop_info in properties.items():
                        prop_type = prop_info.get('type', 'unknown')
                        prop_desc = prop_info.get('description', 'No description')
                        content += f"| `{prop_name}` | {prop_type} | {prop_desc} |\n"
                    
                    content += "\n"
        
        # Add common responses section
        content += """## Common Response Codes

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

## Rate Limiting

- **Default Limit:** 100 requests per minute
- **Authentication Endpoints:** 5 requests per minute
- **Analytics Endpoints:** 20 requests per minute (burst to 30)

Rate limit information is provided in response headers:
- `X-RateLimit-Limit`: Maximum requests allowed
- `X-RateLimit-Remaining`: Remaining requests in current window
- `X-RateLimit-Reset`: Time when the rate limit resets

## Support

For API support and questions:
- **Documentation:** [API Documentation Portal](https://docs.pwc-challenge.com)
- **Issues:** [GitHub Issues](https://github.com/pwc-challenge/issues)
- **Contact:** api-support@pwc-challenge.com

---

*Generated by PwC Data Engineering Challenge API Documentation Generator*
"""

        output_file = self.output_dir / "COMPLETE_API_DOCUMENTATION.md"
        output_file.write_text(content, encoding='utf-8')
        
        return output_file
    
    def _render_endpoint_markdown(self, endpoint: EndpointInfo) -> str:
        """Render individual endpoint as Markdown."""
        method_emoji = {
            "GET": "🟢",
            "POST": "🔵", 
            "PUT": "🟡",
            "DELETE": "🔴",
            "PATCH": "🟠"
        }.get(endpoint.method, "⚪")
        
        content = f"#### {method_emoji} {endpoint.method} `{endpoint.path}`\n\n"
        
        if endpoint.summary:
            content += f"**Summary:** {endpoint.summary}\n\n"
        
        if endpoint.description:
            content += f"{endpoint.description}\n\n"
        
        if endpoint.deprecated:
            content += "⚠️ **DEPRECATED** - This endpoint is deprecated.\n\n"
        
        # Authentication
        if endpoint.security_required:
            content += "🔐 **Authentication Required**\n\n"
        else:
            content += "🔓 **Public Endpoint**\n\n"
        
        # Parameters
        if endpoint.parameters:
            content += "**Parameters:**\n\n"
            content += "| Name | Type | Required | Description |\n"
            content += "|------|------|----------|-------------|\n"
            
            for param in endpoint.parameters:
                required = "✅" if param.get("required", True) else "❌"
                content += f"| `{param['name']}` | {param['type']} | {required} | {param['description']} |\n"
            
            content += "\n"
        
        # Example request
        content += "**Example Request:**\n\n"
        content += "```bash\n"
        auth_header = '-H "Authorization: Bearer $JWT_TOKEN" \\\n  ' if endpoint.security_required else ""
        content += f'curl -X {endpoint.method} "$API_BASE_URL{endpoint.path}" \\\n  {auth_header}-H "Content-Type: application/json"'
        content += "\n```\n\n"
        
        content += "---\n\n"
        return content
    
    def render_postman_collection(self, doc: APIDocumentation) -> Path:
        """Render Postman collection."""
        collection = {
            "info": {
                "name": doc.title,
                "description": doc.description,
                "version": doc.version,
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
                    "value": "https://api.pwc-challenge.com",
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
        
        # Group by tags
        items_by_tag = {}
        for endpoint in doc.endpoints:
            for tag in endpoint.tags:
                if tag not in items_by_tag:
                    items_by_tag[tag] = {
                        "name": tag,
                        "item": []
                    }
                
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
                
                items_by_tag[tag]["item"].append(request_item)
        
        collection["item"] = list(items_by_tag.values())
        
        output_file = self.output_dir / "API_POSTMAN_COLLECTION.json"
        output_file.write_text(json.dumps(collection, indent=2), encoding='utf-8')
        
        return output_file
    
    def render_openapi_spec(self, doc: APIDocumentation) -> Path:
        """Render OpenAPI 3.0 specification."""
        spec = {
            "openapi": "3.0.0",
            "info": {
                "title": doc.title,
                "version": doc.version,
                "description": doc.description,
                "contact": {
                    "name": "PwC Challenge API Support",
                    "email": "api-support@pwc-challenge.com"
                },
                "license": {
                    "name": "MIT",
                    "url": "https://opensource.org/licenses/MIT"
                }
            },
            "servers": [
                {"url": "https://api.pwc-challenge.com", "description": "Production"},
                {"url": "https://staging-api.pwc-challenge.com", "description": "Staging"},
                {"url": "http://localhost:8000", "description": "Development"}
            ],
            "tags": [{"name": tag, "description": f"{tag} related endpoints"} for tag in sorted(doc.tags)],
            "paths": {},
            "components": {
                "schemas": doc.schemas,
                "securitySchemes": {
                    "BearerAuth": {
                        "type": "http",
                        "scheme": "bearer",
                        "bearerFormat": "JWT"
                    },
                    "ApiKeyAuth": {
                        "type": "http", 
                        "scheme": "bearer",
                        "description": "API Key with 'pwc_' prefix"
                    }
                }
            }
        }
        
        # Add paths
        for endpoint in doc.endpoints:
            if endpoint.path not in spec["paths"]:
                spec["paths"][endpoint.path] = {}
            
            method_spec = {
                "summary": endpoint.summary,
                "description": endpoint.description,
                "tags": endpoint.tags,
                "responses": {
                    "200": {"description": "Successful response"},
                    "400": {"description": "Bad request"},
                    "401": {"description": "Unauthorized"},
                    "403": {"description": "Forbidden"},
                    "404": {"description": "Not found"},
                    "500": {"description": "Internal server error"}
                }
            }
            
            if endpoint.security_required:
                method_spec["security"] = [{"BearerAuth": []}]
            
            if endpoint.parameters:
                method_spec["parameters"] = [
                    {
                        "name": param["name"],
                        "in": "query",
                        "required": param.get("required", True),
                        "schema": {"type": param["type"]},
                        "description": param["description"]
                    }
                    for param in endpoint.parameters
                ]
            
            spec["paths"][endpoint.path][endpoint.method.lower()] = method_spec
        
        output_file = self.output_dir / "openapi.json"
        output_file.write_text(json.dumps(spec, indent=2), encoding='utf-8')
        
        return output_file


def main():
    """Main function to generate API documentation."""
    print("PwC Data Engineering Challenge - API Documentation Generator")
    print("=" * 60)
    
    # Setup paths
    script_dir = Path(__file__).parent
    src_dir = script_dir.parent / "src"
    output_dir = script_dir.parent / "docs" / "api"
    
    try:
        # Analyze routes
        print("Analyzing API routes...")
        analyzer = RouteAnalyzer(src_dir)
        api_doc = analyzer.analyze_routes()
        
        print(f"Found {len(api_doc.endpoints)} endpoints across {len(api_doc.tags)} categories")
        
        # Generate documentation
        print("Generating documentation...")
        renderer = DocumentationRenderer(output_dir)
        
        generated_files = []
        generated_files.append(renderer.render_markdown(api_doc))
        generated_files.append(renderer.render_postman_collection(api_doc))
        generated_files.append(renderer.render_openapi_spec(api_doc))
        
        # Generate summary report
        summary = f"""# API Documentation Generation Report

**Generated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

## Statistics

- **Total Endpoints:** {len(api_doc.endpoints)}
- **API Categories:** {len(api_doc.tags)}
- **Data Models:** {len(api_doc.schemas)}

## Endpoints by Method

"""
        
        # Count by method
        method_counts = {}
        for endpoint in api_doc.endpoints:
            method_counts[endpoint.method] = method_counts.get(endpoint.method, 0) + 1
        
        for method, count in sorted(method_counts.items()):
            summary += f"- **{method}:** {count} endpoints\n"
        
        summary += "\n## Endpoints by Category\n\n"
        
        # Count by tag
        tag_counts = {}
        for endpoint in api_doc.endpoints:
            for tag in endpoint.tags:
                tag_counts[tag] = tag_counts.get(tag, 0) + 1
        
        for tag, count in sorted(tag_counts.items()):
            summary += f"- **{tag}:** {count} endpoints\n"
        
        summary += f"""

## Generated Files

"""
        
        for file_path in generated_files:
            file_size = file_path.stat().st_size
            summary += f"- **{file_path.name}** ({file_size:,} bytes)\n"
        
        summary_file = output_dir / "GENERATION_REPORT.md"
        summary_file.write_text(summary, encoding='utf-8')
        
        print("\n" + "=" * 60)
        print("API DOCUMENTATION GENERATION COMPLETE")
        print("=" * 60)
        print(f"Endpoints Analyzed: {len(api_doc.endpoints)}")
        print(f"Categories: {len(api_doc.tags)}")
        print(f"Output Directory: {output_dir}")
        print("\nGenerated Files:")
        for file_path in generated_files + [summary_file]:
            print(f"  ✅ {file_path.name}")
        print("=" * 60)
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main())