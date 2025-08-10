from fastapi import APIRouter, status
from typing import Dict

from de_challenge.core.config import settings

router = APIRouter(tags=["health"])


@router.get("/health", status_code=status.HTTP_200_OK)
async def healthcheck() -> Dict[str, str]:
    return {
        "status": "ok",
        "environment": settings.environment,
        "version": "v1",
    }
