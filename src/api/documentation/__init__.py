"""
PwC Enterprise Data Engineering API - Documentation Module

This module provides comprehensive documentation enhancements for FastAPI,
including custom OpenAPI generation, interactive documentation features,
and enterprise-grade documentation middleware.

Key Features:
- Enhanced OpenAPI 3.0 specification generation
- Custom Swagger UI and ReDoc integration
- Interactive API testing capabilities
- Comprehensive schema documentation
- Performance monitoring for documentation endpoints
- Security-enhanced documentation access

Author: PwC Data Engineering Team
Version: 3.1.0
"""

from .openapi_config import (
    setup_enhanced_openapi_documentation,
    CustomOpenAPIGenerator,
    EnhancedAPIDocsMiddleware
)

from .interactive_docs import (
    create_interactive_docs_router,
    InteractiveAPITester,
    DocumentationAssets
)

from .schema_generator import (
    EnhancedSchemaGenerator,
    BusinessEntitySchemaGenerator,
    APIEndpointAnalyzer
)

__all__ = [
    "setup_enhanced_openapi_documentation",
    "CustomOpenAPIGenerator",
    "EnhancedAPIDocsMiddleware",
    "create_interactive_docs_router",
    "InteractiveAPITester",
    "DocumentationAssets",
    "EnhancedSchemaGenerator",
    "BusinessEntitySchemaGenerator",
    "APIEndpointAnalyzer"
]