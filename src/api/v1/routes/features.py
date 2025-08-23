"""
API Features Router
Provides information about API capabilities and feature overview.
"""
from datetime import datetime
from typing import Any

from fastapi import APIRouter

router = APIRouter(prefix="/features", tags=["features"])


@router.get("/overview", response_model=dict[str, Any])
async def get_api_overview() -> dict[str, Any]:
    """Get comprehensive overview of API features and capabilities."""
    return {
        "api_info": {
            "title": "PwC Data Engineering Challenge - Enterprise API",
            "version": "2.0.0",
            "description": "Enterprise-grade REST API for retail data analytics",
            "documentation": "/docs",
            "graphql_playground": "/api/graphql"
        },
        "authentication": {
            "type": "JWT Bearer Token",
            "fallback": "HTTP Basic Authentication",
            "endpoints": {
                "login": "/api/v1/auth/login",
                "token": "/api/v1/auth/token",
                "refresh": "/api/v1/auth/refresh"
            }
        },
        "api_versions": {
            "v1": {
                "status": "stable",
                "endpoints": [
                    "/api/v1/sales",
                    "/api/v1/datamart/*",
                    "/api/v1/tasks/*",
                    "/api/v1/auth/*",
                    "/api/v1/health",
                    "/api/v1/search/*",
                    "/api/v1/supabase/*"
                ]
            },
            "v2": {
                "status": "enhanced",
                "endpoints": [
                    "/api/v2/sales",
                    "/api/v2/sales/analytics",
                    "/api/v2/sales/export",
                    "/api/v2/sales/schema",
                    "/api/v2/sales/performance/benchmark"
                ],
                "improvements": [
                    "Enhanced filtering options",
                    "Real-time aggregations",
                    "Performance optimizations",
                    "Data quality indicators",
                    "Advanced export capabilities"
                ]
            }
        },
        "data_mart_features": {
            "description": "Access to star schema data marts for analytics",
            "endpoints": {
                "dashboard": "/api/v1/datamart/dashboard/overview",
                "sales_analytics": "/api/v1/datamart/sales/analytics",
                "customer_segments": "/api/v1/datamart/customers/segments",
                "product_performance": "/api/v1/datamart/products/performance",
                "country_performance": "/api/v1/datamart/countries/performance",
                "seasonal_trends": "/api/v1/datamart/trends/seasonal",
                "business_metrics": "/api/v1/datamart/metrics/business"
            },
            "capabilities": [
                "Star schema access",
                "Pre-computed aggregations",
                "Customer segmentation (RFM)",
                "Product performance analytics",
                "Geographic analysis",
                "Seasonal trend analysis",
                "Business KPI dashboards"
            ]
        },
        "async_processing": {
            "description": "Async Request-Reply pattern for long-running operations",
            "pattern": "Submit task -> Get task ID -> Poll status -> Retrieve results",
            "endpoints": {
                "submit_task": "/api/v1/tasks/submit",
                "task_status": "/api/v1/tasks/{task_id}/status",
                "cancel_task": "/api/v1/tasks/{task_id}",
                "list_tasks": "/api/v1/tasks/user/{user_id}",
                "task_statistics": "/api/v1/tasks/statistics"
            },
            "supported_tasks": [
                "generate_comprehensive_report",
                "process_large_dataset",
                "run_advanced_analytics"
            ],
            "features": [
                "Background processing with Celery",
                "Progress tracking",
                "Task cancellation",
                "Result caching",
                "Multi-channel notifications"
            ]
        },
        "graphql_interface": {
            "description": "GraphQL API for flexible data querying",
            "endpoint": "/api/graphql",
            "playground": "/api/graphql",
            "capabilities": [
                "Flexible query structure",
                "Real-time schema introspection",
                "Nested data fetching",
                "Custom filters and pagination",
                "Mutation support for async tasks"
            ],
            "main_types": [
                "Sale",
                "Customer",
                "Product",
                "Country",
                "SalesAnalytics",
                "CustomerSegment",
                "TaskStatus"
            ],
            "example_queries": {
                "basic_sales": "query { sales(pagination: {page: 1, pageSize: 10}) { items { saleId totalAmount product { description } } } }",
                "analytics": "query { salesAnalytics(granularity: MONTHLY) { period totalRevenue } }",
                "segments": "query { customerSegments { segmentName customerCount avgLifetimeValue } }"
            }
        },
        "security_features": {
            "authentication": [
                "JWT Bearer tokens",
                "HTTP Basic auth fallback",
                "Token refresh mechanism"
            ],
            "authorization": [
                "Route-level protection",
                "User-based access control",
                "API key support"
            ],
            "security_headers": [
                "CORS protection",
                "Rate limiting",
                "Input validation",
                "SQL injection prevention"
            ]
        },
        "performance_features": {
            "optimizations": [
                "Parallel query execution",
                "Connection pooling",
                "Query result caching",
                "Lazy data loading",
                "Efficient pagination"
            ],
            "monitoring": [
                "Response time tracking",
                "Query performance metrics",
                "Error rate monitoring",
                "Resource usage tracking"
            ]
        },
        "data_quality": {
            "features": [
                "Real-time quality scoring",
                "Completeness assessment",
                "Data freshness indicators",
                "Validation rules",
                "Quality trend tracking"
            ]
        },
        "export_capabilities": {
            "formats": ["CSV", "Excel", "Parquet", "JSON"],
            "features": [
                "Background processing for large exports",
                "Compression options",
                "Email notifications",
                "Custom filtering",
                "Analytics inclusion"
            ]
        },
        "integration_options": {
            "rest_api": "Standard REST endpoints with OpenAPI documentation",
            "graphql": "Flexible GraphQL interface for complex queries",
            "webhooks": "Event-driven notifications (future)",
            "streaming": "Real-time data streaming (future)"
        },
        "generated_at": datetime.utcnow().isoformat()
    }


@router.get("/endpoints", response_model=list[dict[str, Any]])
async def list_all_endpoints() -> list[dict[str, Any]]:
    """List all available API endpoints with descriptions."""
    endpoints = [
        # Authentication
        {"method": "POST", "path": "/api/v1/auth/login", "description": "User login", "auth_required": False},
        {"method": "POST", "path": "/api/v1/auth/token", "description": "Get JWT token", "auth_required": False},
        {"method": "POST", "path": "/api/v1/auth/refresh", "description": "Refresh JWT token", "auth_required": True},

        # Health & Info
        {"method": "GET", "path": "/api/v1/health", "description": "API health check", "auth_required": False},
        {"method": "GET", "path": "/api/v1/features/overview", "description": "API feature overview", "auth_required": True},

        # Sales V1
        {"method": "GET", "path": "/api/v1/sales", "description": "List sales with basic filtering", "auth_required": True},

        # Sales V2 (Enhanced)
        {"method": "GET", "path": "/api/v2/sales", "description": "Enhanced sales with advanced filtering", "auth_required": True},
        {"method": "POST", "path": "/api/v2/sales/analytics", "description": "Comprehensive sales analytics", "auth_required": True},
        {"method": "POST", "path": "/api/v2/sales/export", "description": "Export sales data", "auth_required": True},
        {"method": "GET", "path": "/api/v2/sales/schema", "description": "Get enhanced schema", "auth_required": True},
        {"method": "GET", "path": "/api/v2/sales/performance/benchmark", "description": "Performance benchmark", "auth_required": True},

        # Data Mart
        {"method": "GET", "path": "/api/v1/datamart/dashboard/overview", "description": "Dashboard KPIs", "auth_required": True},
        {"method": "GET", "path": "/api/v1/datamart/sales/analytics", "description": "Sales analytics", "auth_required": True},
        {"method": "GET", "path": "/api/v1/datamart/customers/segments", "description": "Customer segments", "auth_required": True},
        {"method": "GET", "path": "/api/v1/datamart/customers/{customer_id}/analytics", "description": "Customer analytics", "auth_required": True},
        {"method": "GET", "path": "/api/v1/datamart/products/performance", "description": "Product performance", "auth_required": True},
        {"method": "GET", "path": "/api/v1/datamart/countries/performance", "description": "Country performance", "auth_required": True},
        {"method": "GET", "path": "/api/v1/datamart/trends/seasonal", "description": "Seasonal trends", "auth_required": True},
        {"method": "GET", "path": "/api/v1/datamart/metrics/business", "description": "Business metrics", "auth_required": True},

        # Async Tasks
        {"method": "POST", "path": "/api/v1/tasks/submit", "description": "Submit async task", "auth_required": True},
        {"method": "GET", "path": "/api/v1/tasks/{task_id}/status", "description": "Get task status", "auth_required": True},
        {"method": "DELETE", "path": "/api/v1/tasks/{task_id}", "description": "Cancel task", "auth_required": True},
        {"method": "GET", "path": "/api/v1/tasks/user/{user_id}", "description": "List user tasks", "auth_required": True},
        {"method": "GET", "path": "/api/v1/tasks/statistics", "description": "Task statistics", "auth_required": True},
        {"method": "POST", "path": "/api/v1/tasks/reports/generate", "description": "Generate report", "auth_required": True},
        {"method": "POST", "path": "/api/v1/tasks/etl/process", "description": "Process dataset", "auth_required": True},
        {"method": "POST", "path": "/api/v1/tasks/analytics/run", "description": "Run analytics", "auth_required": True},

        # GraphQL
        {"method": "POST", "path": "/api/graphql", "description": "GraphQL endpoint", "auth_required": True},
        {"method": "GET", "path": "/api/graphql", "description": "GraphQL playground", "auth_required": True},

        # Search (if enabled)
        {"method": "GET", "path": "/api/v1/search", "description": "Vector search", "auth_required": True},

        # Supabase Integration
        {"method": "GET", "path": "/api/v1/supabase/*", "description": "Supabase operations", "auth_required": True},
    ]

    return endpoints


@router.get("/migration-guide", response_model=dict[str, Any])
async def get_migration_guide() -> dict[str, Any]:
    """Get migration guide from v1 to v2 API."""
    return {
        "title": "API v1 to v2 Migration Guide",
        "overview": "Guide for migrating from v1 to v2 endpoints with enhanced features",
        "breaking_changes": [
            "Enhanced sale item schema with additional fields",
            "Improved filter parameter structure",
            "Response format includes metadata and quality indicators"
        ],
        "new_features": [
            "Real-time aggregations",
            "Data quality scoring",
            "Advanced export options",
            "Performance benchmarking",
            "Enhanced analytics"
        ],
        "migration_steps": {
            "1": "Update base URL from /api/v1/sales to /api/v2/sales",
            "2": "Review new response schema with additional fields",
            "3": "Update filter parameters to use new structure",
            "4": "Handle new metadata fields in responses",
            "5": "Test with new performance and quality indicators"
        },
        "compatibility": {
            "v1_support": "Fully maintained - no deprecation planned",
            "v2_advantages": [
                "30-50% performance improvement",
                "Enhanced filtering capabilities",
                "Real-time data quality indicators",
                "Advanced export formats",
                "Better error handling"
            ]
        },
        "examples": {
            "v1_request": "GET /api/v1/sales?date_from=2023-01-01&country=UK",
            "v2_request": "GET /api/v2/sales?date_from=2023-01-01&countries=UK&include_aggregations=true",
            "v1_response": "Basic paginated sales list",
            "v2_response": "Enhanced sales with analytics, quality score, and metadata"
        }
    }


@router.get("/performance", response_model=dict[str, Any])
async def get_performance_info() -> dict[str, Any]:
    """Get API performance characteristics and optimization tips."""
    return {
        "performance_metrics": {
            "typical_response_times": {
                "simple_queries": "< 200ms",
                "with_aggregations": "< 500ms",
                "complex_analytics": "< 2s",
                "large_exports": "5-30 minutes (async)"
            },
            "throughput": {
                "concurrent_requests": "100+",
                "requests_per_second": "500+",
                "max_page_size": 100
            }
        },
        "optimization_tips": [
            "Use pagination for large datasets",
            "Apply filters to reduce data volume",
            "Use async tasks for heavy operations",
            "Enable compression for large responses",
            "Cache frequently accessed data"
        ],
        "rate_limits": {
            "authenticated_users": "1000 requests/hour",
            "guest_users": "100 requests/hour",
            "burst_allowance": "50 requests/minute"
        },
        "monitoring": {
            "response_time_tracking": "All endpoints monitored",
            "error_rate_tracking": "Real-time alerting",
            "resource_monitoring": "CPU, memory, database connections"
        }
    }
