from typing import Annotated

from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.security import HTTPBasic, HTTPBasicCredentials
from pydantic import BaseModel, Field

from api.v1.services.authentication_service import AuthenticationService


class LoginRequest(BaseModel):
    username: str = Field(..., description="Username for authentication")
    password: str = Field(..., description="Password for authentication", min_length=8)


class TokenResponse(BaseModel):
    access_token: str = Field(..., description="JWT access token")
    token_type: str = Field(default="bearer", description="Token type")
    expires_in: int = Field(..., description="Token expiration time in seconds")
    permissions: list[str] = Field(default=[], description="User permissions")


class RefreshTokenRequest(BaseModel):
    token: str = Field(..., description="Current access token to refresh")


router = APIRouter(tags=["Authentication"])
auth_service = AuthenticationService()
security = HTTPBasic()


@router.post("/auth/login", response_model=TokenResponse, status_code=status.HTTP_200_OK)
async def login(login_request: LoginRequest) -> TokenResponse:
    if not await auth_service.authenticate_user(login_request.username, login_request.password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )

    permissions = ["read", "write", "admin"] if login_request.username == "admin" else ["read"]
    access_token = await auth_service.create_user_token(login_request.username, permissions)

    return TokenResponse(
        access_token=access_token,
        token_type="bearer",
        expires_in=auth_service.security_config.jwt_expiration_hours * 3600,
        permissions=permissions
    )


@router.post("/auth/token", response_model=TokenResponse, status_code=status.HTTP_200_OK)
async def create_token_basic_auth(
    credentials: Annotated[HTTPBasicCredentials, Depends(security)]
) -> TokenResponse:
    if not await auth_service.authenticate_user(credentials.username, credentials.password):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Basic"},
        )

    permissions = ["read", "write", "admin"] if credentials.username == "admin" else ["read"]
    access_token = await auth_service.create_user_token(credentials.username, permissions)

    return TokenResponse(
        access_token=access_token,
        token_type="bearer",
        expires_in=auth_service.security_config.jwt_expiration_hours * 3600,
        permissions=permissions
    )


@router.post("/auth/refresh", response_model=TokenResponse, status_code=status.HTTP_200_OK)
async def refresh_token(refresh_request: RefreshTokenRequest) -> TokenResponse:
    token_data = await auth_service.verify_token(refresh_request.token)
    if not token_data:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    new_token = await auth_service.create_user_token(token_data.username, token_data.permissions)

    return TokenResponse(
        access_token=new_token,
        token_type="bearer",
        expires_in=auth_service.security_config.jwt_expiration_hours * 3600,
        permissions=token_data.permissions
    )


@router.post("/auth/validate", status_code=status.HTTP_200_OK)
async def validate_token(token: str) -> dict:
    token_data = await auth_service.verify_token(token)
    if not token_data:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid token",
        )

    return {
        "valid": True,
        "username": token_data.username,
        "expires_at": token_data.expires_at,
        "permissions": token_data.permissions
    }
