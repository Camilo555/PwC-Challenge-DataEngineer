
from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials

from de_challenge.api.v1.routes.health import router as health_router
from de_challenge.api.v1.routes.sales import router as sales_router
from de_challenge.api.v1.routes.search import router as search_router  # type: ignore
from de_challenge.api.v1.routes.supabase import router as supabase_router
from de_challenge.core.config import settings
from de_challenge.core.logging import get_logger

security = HTTPBasic()
logger = get_logger(__name__)

app = FastAPI(
    title="Retail ETL API",
    version="1.0.0",
    description="APIs for querying sales and managing ETL jobs.",
)


def verify_basic_auth(credentials: HTTPBasicCredentials = Depends(security)) -> None:
    if (
        credentials.username != settings.basic_auth_username
        or credentials.password != settings.basic_auth_password
    ):
        # Do not reveal which field failed
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Unauthorized",
            headers={"WWW-Authenticate": "Basic"},
        )


@app.on_event("startup")
async def startup_event() -> None:
    logger.info("API starting up", extra={"env": settings.environment})


@app.get("/", tags=["root"], status_code=status.HTTP_200_OK)
async def root() -> dict[str, str]:
    return {"message": "Retail ETL API is running"}

@app.get("/health", tags=["health"], status_code=status.HTTP_200_OK)
async def health() -> dict[str, str]:
    return {"status": "ok", "environment": settings.environment, "version": "v1"}


# Mount versioned routers
app.include_router(health_router, prefix="/api/v1")
app.include_router(sales_router, prefix="/api/v1", dependencies=[Depends(verify_basic_auth)])
app.include_router(supabase_router, prefix="/api/v1", dependencies=[Depends(verify_basic_auth)])
if settings.enable_vector_search:
    app.include_router(search_router, prefix="/api/v1", dependencies=[Depends(verify_basic_auth)])
