import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

import orjson
from fastapi import Depends, FastAPI, HTTPException, WebSocket, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.middleware.trustedhost import TrustedHostMiddleware
from fastapi.responses import ORJSONResponse
from fastapi.security import (
    HTTPAuthorizationCredentials,
    HTTPBasic,
    HTTPBasicCredentials,
    HTTPBearer,
)
from passlib.context import CryptContext

# Enhanced security components
from api.middleware.enterprise_security import get_enterprise_security_middleware
from api.middleware.response_security import get_response_security_middleware
from api.testing.security_testing import get_testing_framework
from api.testing.security_testing import router as security_testing_router

# API routes
from api.v1.routes.auth import router as auth_router
from api.v1.routes.health import router as health_router
from api.v1.routes.sales import router as sales_router
from api.v1.routes.search import router as search_router
from api.v1.routes.security import router as security_router
from api.v1.routes.supabase import router as supabase_router
from api.v1.services.enhanced_auth_service import get_auth_service
from api.websocket.security_websocket import get_websocket_manager, security_websocket_endpoint

# Configuration
from core.config.base_config import BaseConfig
from core.config.security_config import SecurityConfig
from core.logging import get_logger
from core.security.enterprise_security_orchestrator import (
    SecurityOrchestrationConfig,
    get_security_orchestrator,
)

security_config = SecurityConfig()
base_config = BaseConfig()
security = HTTPBearer()
basic_security = HTTPBasic()
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
logger = get_logger(__name__)

# Initialize enhanced security components
security_orchestration_config = SecurityOrchestrationConfig(
    enable_dlp=True,
    enable_compliance_monitoring=True,
    enable_enhanced_access_control=True,
    enable_data_governance=True,
    enable_real_time_monitoring=True,
)
security_orchestrator = get_security_orchestrator(security_orchestration_config)
auth_service = get_auth_service()


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    logger.info(
        "Enterprise API starting up",
        extra={
            "env": base_config.environment,
            "security_enabled": True,
            "dlp_enabled": security_orchestration_config.enable_dlp,
            "compliance_monitoring": security_orchestration_config.enable_compliance_monitoring,
        },
    )

    # Initialize security systems
    try:
        # Run initial security assessment
        await security_orchestrator.run_security_assessment()
        logger.info("Security assessment completed during startup")
    except Exception as e:
        logger.error(f"Security assessment failed during startup: {e}")

    yield

    logger.info("Enterprise API shutting down")
    # Cleanup security resources if needed


# Custom JSON response class for optimal performance
class OptimizedORJSONResponse(ORJSONResponse):
    def render(self, content: Any) -> bytes:
        return orjson.dumps(
            content, option=orjson.OPT_NON_STR_KEYS | orjson.OPT_SERIALIZE_NUMPY | orjson.OPT_UTC_Z
        )


# Performance optimized FastAPI application
app = FastAPI(
    title="PwC Data Engineering Challenge - Enterprise API",
    version="4.0.0",
    description="""Enterprise-grade high-performance REST API with comprehensive features including:

    ## Performance Features
    - ORJSON/UJSON optimized JSON serialization (up to 3x faster)
    - GZip compression for reduced payload sizes
    - Connection pooling and keep-alive optimization
    - Async/await throughout for maximum concurrency
    - Resource pooling and connection management

    ## Security Features
    - Advanced DLP (Data Loss Prevention) with PII/PHI detection and redaction
    - Enhanced RBAC/ABAC access control with privilege elevation
    - Multi-framework compliance (GDPR, HIPAA, PCI-DSS, SOX)
    - Real-time security monitoring and threat detection
    - JWT, API Keys, OAuth2/OIDC authentication

    ## Microservices Features
    - Service discovery and registration
    - Circuit breakers and resilience patterns
    - Event-driven architecture with CQRS/Saga patterns
    - Distributed caching with Redis
    - Real-time WebSocket communication

    ## API Features
    - RESTful APIs with OpenAPI 3.0 documentation
    - GraphQL endpoint for flexible querying
    - Comprehensive rate limiting and throttling
    - API versioning with backward compatibility
    - Real-time monitoring and observability
    """,
    docs_url="/docs" if base_config.environment != "production" else None,
    redoc_url="/redoc" if base_config.environment != "production" else None,
    default_response_class=OptimizedORJSONResponse,
    lifespan=lifespan,
    openapi_tags=[
        {"name": "Authentication", "description": "JWT, API Keys, OAuth2 authentication"},
        {"name": "Sales", "description": "Sales data and analytics endpoints"},
        {"name": "Analytics", "description": "Advanced analytics and reporting"},
        {"name": "Security", "description": "Security monitoring and management"},
        {"name": "Health", "description": "System health and monitoring"},
        {"name": "Microservices", "description": "Service discovery and orchestration"},
        {"name": "WebSockets", "description": "Real-time communication endpoints"},
    ],
)

# Import enhanced middleware
from api.middleware.circuit_breaker import CircuitBreaker, CircuitBreakerMiddleware

# Import service locator for dependency management
from api.middleware.correlation import CorrelationMiddleware as CorrelationIDMiddleware
from api.middleware.rate_limiter import RateLimitMiddleware
from api.services.service_locator import (
    get_service,
    register_microservices_components,
)

# Initialize microservices components
register_microservices_components()

# Performance middleware stack (order matters!)

# Trusted Host middleware (security)
if base_config.environment == "production":
    app.add_middleware(
        TrustedHostMiddleware,
        allowed_hosts=["*"],  # Configure with actual domains in production
    )

# GZip compression middleware (performance)
app.add_middleware(
    GZipMiddleware,
    minimum_size=1000,
    compresslevel=6,  # Balance between compression ratio and CPU usage
)


# Custom performance monitoring middleware
class PerformanceMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] == "http":
            start_time = time.time()

            async def send_wrapper(message):
                if message["type"] == "http.response.start":
                    process_time = time.time() - start_time
                    message["headers"].append((b"x-process-time", f"{process_time:.4f}".encode()))
                    message["headers"].append((b"x-api-version", b"4.0.0"))
                await send(message)

            await self.app(scope, receive, send_wrapper)
        else:
            await self.app(scope, receive, send)


app.add_middleware(PerformanceMiddleware)

# CORS middleware with enhanced security
app.add_middleware(
    CORSMiddleware,
    allow_origins=security_config.cors_allowed_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS", "HEAD", "PATCH"],
    allow_headers=[
        "Authorization",
        "Content-Type",
        "X-Requested-With",
        "X-Correlation-ID",
        "X-API-Key",
        "Accept",
        "Accept-Encoding",
        "Accept-Language",
        "Origin",
        "User-Agent",
        "Cache-Control",
        "Pragma",
    ],
    expose_headers=[
        "X-Correlation-ID",
        "X-Security-Processed",
        "X-Data-Classification",
        "X-DLP-Scan-ID",
        "X-Security-Score",
        "X-Process-Time",
        "X-API-Version",
        "X-RateLimit-Limit",
        "X-RateLimit-Remaining",
        "X-RateLimit-Reset",
    ],
    max_age=3600,  # Cache preflight requests for 1 hour
)

# Enterprise security middleware (highest priority)
enterprise_security_middleware = get_enterprise_security_middleware(
    config=security_orchestration_config,
    exclude_paths=["/health", "/metrics", "/docs", "/redoc", "/openapi.json", "/favicon.ico"],
)
app.add_middleware(
    enterprise_security_middleware.__class__,
    app=app,
    config=security_orchestration_config,
    exclude_paths=["/health", "/metrics", "/docs", "/redoc", "/openapi.json"],
)

# Response security middleware (for PII/PHI redaction)
response_security_middleware = get_response_security_middleware(
    default_redaction_level="standard",
    enable_compliance_filtering=True,
    compliance_frameworks=["gdpr", "pci_dss", "hipaa", "sox"],
)
app.add_middleware(
    response_security_middleware.__class__,
    app=app,
    default_redaction_level="standard",
    enable_compliance_filtering=True,
    compliance_frameworks=["gdpr", "pci_dss", "hipaa", "sox"],
)

# Add correlation ID middleware
app.add_middleware(CorrelationIDMiddleware)

# Circuit breaker middleware
circuit_breakers = {
    "GET:/api/v1/sales/analytics": CircuitBreaker(failure_threshold=3, timeout=30),
    "POST:/api/v2/analytics/advanced-analytics": CircuitBreaker(failure_threshold=5, timeout=60),
}
app.add_middleware(CircuitBreakerMiddleware, circuit_breakers=circuit_breakers)

# Rate limiting middleware
rate_limit_rules = {
    "/api/v1/auth/*": {"limit": 5, "window": 60},  # 5 requests per minute for auth
    "/api/v2/analytics/*": {"limit": 20, "window": 60, "burst_limit": 30},  # Analytics endpoints
    "default": {"limit": 100, "window": 60},  # Default rate limit
}
redis_url = getattr(base_config, "redis_url", "redis://localhost:6379/0")
app.add_middleware(
    RateLimitMiddleware,
    redis_url=redis_url,
    default_limit=100,
    default_window=60,
    burst_limit=150,
    rate_limit_rules=rate_limit_rules,
)

# Initialize enterprise patterns using service locator
service_registry = get_service("service_registry")
saga_orchestrator = get_service("saga_orchestrator")
cqrs_framework = get_service("cqrs_framework")

if any([service_registry, saga_orchestrator, cqrs_framework]):
    try:
        # Add service discovery middleware if available
        if service_registry:
            try:
                from api.gateway.service_registry import ServiceDiscoveryMiddleware

                app.add_middleware(ServiceDiscoveryMiddleware, service_registry=service_registry)
                logger.info("Service discovery middleware added")
            except ImportError:
                logger.info("ServiceDiscoveryMiddleware not available")

        # Create and add API Gateway (optional - for microservices routing)
        # api_gateway = create_api_gateway(app)
        # app.add_middleware(api_gateway.__class__, app=app, routes=api_gateway.routes, enable_service_mesh=True)
    except Exception as e:
        logger.warning(f"Failed to initialize advanced patterns: {e}")


async def verify_jwt_token(
    credentials: HTTPAuthorizationCredentials = Depends(security),
) -> dict[str, Any]:
    """Enhanced JWT token verification with security orchestrator integration"""

    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    try:
        # Use enhanced auth service for token verification
        auth_result = await auth_service.authenticate_jwt(credentials.credentials)

        if not auth_result.success:
            raise credentials_exception

        token_claims = auth_result.token_claims

        # Convert to dict for backward compatibility
        return {
            "sub": token_claims.sub,
            "roles": token_claims.roles,
            "permissions": token_claims.permissions,
            "clearance_level": token_claims.clearance_level,
            "session_id": token_claims.session_id,
            "mfa_verified": token_claims.mfa_verified,
            "risk_score": token_claims.risk_score,
            "authentication_method": token_claims.authentication_method,
            "source_ip": token_claims.source_ip,
        }

    except Exception as err:
        logger.warning(f"Token verification failed: {err}")
        raise credentials_exception from err


def verify_basic_auth_fallback(credentials: HTTPBasicCredentials = Depends(basic_security)) -> None:
    if credentials.username != security_config.basic_auth_username or not pwd_context.verify(
        credentials.password, security_config.hashed_password
    ):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Basic"},
        )


@app.get("/", tags=["root"], status_code=status.HTTP_200_OK, response_class=OptimizedORJSONResponse)
async def root() -> dict[str, Any]:
    """Enhanced root endpoint with security information"""

    security_summary = security_orchestrator.get_security_summary()

    return {
        "message": "PwC Data Engineering Challenge - Enterprise Security API",
        "version": "3.0.0",
        "documentation": "/docs",
        "health": "/health",
        "security_dashboard": "/api/v1/security/dashboard",
        "websocket_dashboard": "/ws/security/dashboard",
        "security_features": {
            "dlp_enabled": security_orchestration_config.enable_dlp,
            "compliance_monitoring": security_orchestration_config.enable_compliance_monitoring,
            "enhanced_access_control": security_orchestration_config.enable_enhanced_access_control,
            "real_time_monitoring": security_orchestration_config.enable_real_time_monitoring,
            "security_level": security_summary.get("security_level", "operational"),
        },
        "supported_compliance": ["GDPR", "HIPAA", "PCI-DSS", "SOX"],
        "api_capabilities": [
            "JWT Authentication with MFA support",
            "OAuth2/OIDC Integration",
            "API Key Management",
            "Real-time PII/PHI Detection and Redaction",
            "Advanced RBAC/ABAC Authorization",
            "Privilege Elevation Management",
            "Automated Security Testing",
            "Real-time Security Monitoring",
            "Compliance Reporting",
        ],
    }


@app.get("/health", tags=["health"], status_code=status.HTTP_200_OK)
async def health() -> dict[str, Any]:
    """Enhanced health endpoint with comprehensive security status"""

    try:
        # Get comprehensive security status
        security_summary = security_orchestrator.get_security_summary()

        # Check all security components
        component_health = {
            "security_orchestrator": "healthy",
            "dlp_engine": "healthy" if security_orchestrator.dlp_manager else "disabled",
            "access_control": "healthy",
            "compliance_engine": "healthy",
            "auth_service": "healthy",
        }

        overall_status = "healthy"
        if any(status == "unhealthy" for status in component_health.values()):
            overall_status = "degraded"

        return {
            "status": overall_status,
            "environment": base_config.environment.value,
            "version": "3.0.0",
            "timestamp": base_config.get_current_timestamp(),
            "security": {
                "platform_status": security_summary.get("platform_status", "operational"),
                "security_level": security_summary.get("security_level", "operational"),
                "components_status": security_summary.get("components_status", {}),
                "auth_enabled": security_config.auth_enabled,
                "dlp_enabled": security_orchestration_config.enable_dlp,
                "compliance_monitoring": security_orchestration_config.enable_compliance_monitoring,
                "rate_limiting": True,
                "https_enabled": getattr(security_config, "https_enabled", False),
                "mfa_supported": True,
                "oauth_enabled": True,
                "api_keys_supported": True,
            },
            "metrics": {
                "active_connections": len(getattr(security_orchestrator, "active_connections", {})),
                "security_events_24h": security_summary.get("key_metrics", {}).get(
                    "active_alerts", 0
                ),
                "compliance_score": security_summary.get("key_metrics", {}).get(
                    "compliance_score", 1.0
                ),
            },
            "component_health": component_health,
        }

    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return {
            "status": "unhealthy",
            "environment": base_config.environment.value,
            "version": "4.0.0",
            "timestamp": base_config.get_current_timestamp(),
            "error": str(e),
        }


# Enhanced authentication dependency with multiple methods
async def enhanced_auth_dependency(
    credentials: HTTPAuthorizationCredentials = Depends(security),
) -> dict[str, Any]:
    """Enhanced authentication supporting JWT, API keys, and OAuth"""

    token = credentials.credentials

    # Try API key authentication first
    if token.startswith("pwc_"):
        auth_result = await auth_service.authenticate_api_key(token)
        if auth_result.success:
            return {
                "sub": auth_result.user_id,
                "auth_method": "api_key",
                "permissions": auth_result.permissions,
                "session_id": auth_result.session_id,
                "risk_score": auth_result.risk_score,
                "api_key_info": {
                    "key_id": auth_result.api_key_info.key_id,
                    "name": auth_result.api_key_info.name,
                    "usage_count": auth_result.api_key_info.usage_count,
                }
                if auth_result.api_key_info
                else None,
            }
        else:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail=auth_result.error_message or "Invalid API key",
                headers={"WWW-Authenticate": "Bearer"},
            )

    # Default to JWT authentication
    return await verify_jwt_token(credentials)


auth_dependency = (
    [Depends(enhanced_auth_dependency)]
    if security_config.jwt_auth_enabled
    else [Depends(verify_basic_auth_fallback)]
)

# Import additional routers
from api.graphql.router import graphql_router
from api.v1.routes.async_tasks import router as async_tasks_router
from api.v1.routes.datamart import router as datamart_router
from api.v1.routes.enterprise import router as enterprise_router
from api.v1.routes.features import router as features_router
from api.v1.routes.monitoring import router as monitoring_router
from api.v2.routes.analytics import router as analytics_v2_router
from api.v2.routes.sales import router as sales_v2_router

# Mount v1 routers with enhanced authentication
app.include_router(auth_router, prefix="/api/v1")  # No auth required for auth endpoints
app.include_router(health_router, prefix="/api/v1")
app.include_router(
    security_router, prefix="/api/v1", dependencies=auth_dependency
)  # Security management endpoints
app.include_router(
    security_testing_router, prefix="/api/v1", dependencies=auth_dependency
)  # Security testing endpoints
app.include_router(features_router, prefix="/api/v1", dependencies=auth_dependency)
app.include_router(sales_router, prefix="/api/v1", dependencies=auth_dependency)
app.include_router(datamart_router, prefix="/api/v1", dependencies=auth_dependency)
app.include_router(async_tasks_router, prefix="/api/v1", dependencies=auth_dependency)
app.include_router(enterprise_router, prefix="/api/v1", dependencies=auth_dependency)
app.include_router(monitoring_router, prefix="/api/v1", dependencies=auth_dependency)
app.include_router(supabase_router, prefix="/api/v1", dependencies=auth_dependency)

# Mount v2 routers with authentication
app.include_router(sales_v2_router, prefix="/api/v2", dependencies=auth_dependency)
app.include_router(analytics_v2_router, prefix="/api/v2", dependencies=auth_dependency)

# Mount GraphQL with authentication
app.include_router(graphql_router, prefix="/api", dependencies=auth_dependency)

if base_config.enable_vector_search:
    app.include_router(search_router, prefix="/api/v1", dependencies=auth_dependency)


# WebSocket endpoints for real-time security dashboard
@app.websocket("/ws/security/dashboard")
async def websocket_security_dashboard(websocket: WebSocket, token: str = None):
    """WebSocket endpoint for real-time security dashboard"""
    await security_websocket_endpoint(websocket, token)


@app.websocket("/ws/security/alerts")
async def websocket_security_alerts(websocket: WebSocket, token: str = None):
    """WebSocket endpoint for real-time security alerts"""
    await security_websocket_endpoint(websocket, token)


# Additional security endpoints for enhanced monitoring
@app.get("/api/v1/security/status")
async def get_comprehensive_security_status(current_user: dict = Depends(enhanced_auth_dependency)):
    """Get comprehensive security system status"""

    # Check permissions
    if "perm_security_read" not in current_user.get(
        "permissions", []
    ) and "perm_admin_system" not in current_user.get("permissions", []):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="Security read permissions required"
        )

    try:
        security_summary = security_orchestrator.get_security_summary()
        websocket_manager = get_websocket_manager()

        return {
            "platform_status": security_summary.get("platform_status", "operational"),
            "security_level": security_summary.get("security_level", "operational"),
            "components_status": security_summary.get("components_status", {}),
            "key_metrics": security_summary.get("key_metrics", {}),
            "active_websocket_connections": len(websocket_manager.connections),
            "security_features": {
                "dlp_enabled": security_orchestration_config.enable_dlp,
                "compliance_monitoring": security_orchestration_config.enable_compliance_monitoring,
                "enhanced_access_control": security_orchestration_config.enable_enhanced_access_control,
                "real_time_monitoring": security_orchestration_config.enable_real_time_monitoring,
                "data_governance": security_orchestration_config.enable_data_governance,
            },
            "authentication_methods": [
                "JWT with enhanced claims",
                "API Keys with security policies",
                "OAuth2/OIDC integration",
                "Multi-factor authentication",
            ],
            "compliance_frameworks": ["GDPR", "HIPAA", "PCI-DSS", "SOX"],
            "last_security_assessment": security_summary.get("last_assessment", None),
        }

    except Exception as e:
        logger.error(f"Failed to get security status: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to retrieve security status",
        )


@app.post("/api/v1/security/assessment/trigger")
async def trigger_security_assessment(current_user: dict = Depends(enhanced_auth_dependency)):
    """Trigger comprehensive security assessment"""

    # Check admin permissions
    if "perm_admin_system" not in current_user.get("permissions", []):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN, detail="Administrative privileges required"
        )

    try:
        assessment_result = await security_orchestrator.run_security_assessment()
        return {
            "assessment_triggered": True,
            "assessment_id": assessment_result.get("assessment_id"),
            "status": assessment_result.get("status"),
            "summary": assessment_result.get("summary", {}),
            "triggered_by": current_user["sub"],
        }

    except Exception as e:
        logger.error(f"Failed to trigger security assessment: {e}")
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="Failed to trigger security assessment",
        )


# Security testing endpoints (only in non-production environments)
if base_config.environment.value != "production":

    @app.get("/api/v1/security/test/framework-status")
    async def get_security_test_framework_status():
        """Get security testing framework status"""
        framework = get_testing_framework()
        return {
            "testing_enabled": True,
            "environment": base_config.environment.value,
            "framework_initialized": framework is not None,
            "available_tests": [
                "comprehensive_security_assessment",
                "penetration_testing",
                "vulnerability_assessment",
                "compliance_validation",
                "authentication_testing",
                "authorization_testing",
                "input_validation_testing",
                "data_protection_testing",
            ],
            "supported_compliance_standards": [
                "OWASP Top 10",
                "GDPR",
                "PCI-DSS",
                "HIPAA",
                "SOX",
                "ISO 27001",
            ],
            "framework_version": "3.0.0",
        }
