
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import (
    HTTPAuthorizationCredentials,
    HTTPBasic,
    HTTPBasicCredentials,
    HTTPBearer,
)
from jose import JWTError, jwt
from passlib.context import CryptContext

from api.v1.routes.auth import router as auth_router
from api.v1.routes.health import router as health_router
from api.v1.routes.sales import router as sales_router
from api.v1.routes.search import router as search_router
from api.v1.routes.supabase import router as supabase_router
from core.config.base_config import BaseConfig
from core.config.security_config import SecurityConfig
from core.logging import get_logger

security_config = SecurityConfig()
base_config = BaseConfig()
security = HTTPBearer()
basic_security = HTTPBasic()
pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
logger = get_logger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    logger.info("API starting up", extra={"env": base_config.environment})
    yield
    logger.info("API shutting down")


app = FastAPI(
    title="PwC Data Engineering Challenge - Enterprise API",
    version="2.0.0",
    description="Enterprise-grade REST API for retail data analytics with JWT authentication, rate limiting, and comprehensive security",
    docs_url="/docs" if base_config.environment != "production" else None,
    redoc_url="/redoc" if base_config.environment != "production" else None,
    lifespan=lifespan,
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=security_config.cors_allowed_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["*"],
)


async def verify_jwt_token(credentials: HTTPAuthorizationCredentials = Depends(security)) -> dict[str, Any]:
    credentials_exception = HTTPException(
        status_code=status.HTTP_401_UNAUTHORIZED,
        detail="Could not validate credentials",
        headers={"WWW-Authenticate": "Bearer"},
    )

    try:
        payload: dict[str, Any] = jwt.decode(
            credentials.credentials,
            security_config.jwt_secret_key,
            algorithms=[security_config.jwt_algorithm]
        )
        username = payload.get("sub")
        if username is None:
            raise credentials_exception
        return payload
    except JWTError as err:
        raise credentials_exception from err


def verify_basic_auth_fallback(credentials: HTTPBasicCredentials = Depends(basic_security)) -> None:
    if (credentials.username != security_config.basic_auth_username or
        not pwd_context.verify(credentials.password, security_config.hashed_password)):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid authentication credentials",
            headers={"WWW-Authenticate": "Basic"},
        )


@app.get("/", tags=["root"], status_code=status.HTTP_200_OK)
async def root() -> dict[str, str]:
    return {
        "message": "PwC Data Engineering Challenge API",
        "version": "2.0.0",
        "documentation": "/docs",
        "health": "/health"
    }


@app.get("/health", tags=["health"], status_code=status.HTTP_200_OK)
async def health() -> dict[str, Any]:
    return {
        "status": "healthy",
        "environment": base_config.environment.value,
        "version": "2.0.0",
        "timestamp": base_config.get_current_timestamp(),
        "security": {
            "auth_enabled": security_config.auth_enabled,
            "rate_limiting": getattr(security_config, "rate_limiting_enabled", False),
            "https_enabled": getattr(security_config, "https_enabled", False)
        }
    }


# Authentication dependency
auth_dependency = [Depends(verify_jwt_token)] if security_config.jwt_auth_enabled else [Depends(verify_basic_auth_fallback)]

# Import additional routers
from api.v1.routes.datamart import router as datamart_router
from api.v1.routes.async_tasks import router as async_tasks_router
from api.v1.routes.features import router as features_router
from api.v2.routes.sales import router as sales_v2_router
from api.graphql.router import graphql_router

# Mount v1 routers with proper authentication
app.include_router(auth_router, prefix="/api/v1")  # No auth required for auth endpoints
app.include_router(health_router, prefix="/api/v1")
app.include_router(features_router, prefix="/api/v1", dependencies=auth_dependency)
app.include_router(sales_router, prefix="/api/v1", dependencies=auth_dependency)
app.include_router(datamart_router, prefix="/api/v1", dependencies=auth_dependency)
app.include_router(async_tasks_router, prefix="/api/v1", dependencies=auth_dependency)
app.include_router(supabase_router, prefix="/api/v1", dependencies=auth_dependency)

# Mount v2 routers with authentication
app.include_router(sales_v2_router, prefix="/api/v2", dependencies=auth_dependency)

# Mount GraphQL with authentication
app.include_router(graphql_router, prefix="/api", dependencies=auth_dependency)

if base_config.enable_vector_search:
    app.include_router(search_router, prefix="/api/v1", dependencies=auth_dependency)
