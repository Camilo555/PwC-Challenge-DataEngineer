
from fastapi import APIRouter, status

from core.config import settings

router = APIRouter(tags=["health"])


@router.get("/health", status_code=status.HTTP_200_OK)
async def healthcheck() -> dict[str, str]:
    return {
        "status": "ok",
        "environment": settings.environment,
        "version": "v1",
    }
