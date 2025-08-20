from datetime import datetime, timedelta
from typing import Optional

from jose import JWTError, jwt
from passlib.context import CryptContext
from pydantic import BaseModel

from core.config.security_config import SecurityConfig


class TokenData(BaseModel):
    username: Optional[str] = None
    expires_at: Optional[datetime] = None
    permissions: list[str] = []


class AuthenticationService:
    def __init__(self):
        self.security_config = SecurityConfig()
        self.pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
    
    def verify_password(self, plain_password: str, hashed_password: str) -> bool:
        return self.pwd_context.verify(plain_password, hashed_password)
    
    def get_password_hash(self, password: str) -> str:
        return self.pwd_context.hash(password)
    
    def create_access_token(self, data: dict, expires_delta: Optional[timedelta] = None) -> str:
        to_encode = data.copy()
        if expires_delta:
            expire = datetime.utcnow() + expires_delta
        else:
            expire = datetime.utcnow() + timedelta(hours=self.security_config.jwt_expiration_hours)
        
        to_encode.update({"exp": expire})
        encoded_jwt = jwt.encode(
            to_encode, 
            self.security_config.jwt_secret_key, 
            algorithm=self.security_config.jwt_algorithm
        )
        return encoded_jwt
    
    def verify_token(self, token: str) -> Optional[TokenData]:
        try:
            payload = jwt.decode(
                token, 
                self.security_config.jwt_secret_key, 
                algorithms=[self.security_config.jwt_algorithm]
            )
            username: str = payload.get("sub")
            if username is None:
                return None
            
            return TokenData(
                username=username,
                expires_at=datetime.fromtimestamp(payload.get("exp", 0)),
                permissions=payload.get("permissions", [])
            )
        except JWTError:
            return None
    
    def authenticate_user(self, username: str, password: str) -> bool:
        if username != self.security_config.admin_username:
            return False
        return self.verify_password(password, self.security_config.hashed_password)
    
    def create_user_token(self, username: str, permissions: list[str] = None) -> str:
        if permissions is None:
            permissions = ["read", "write"]
        
        token_data = {
            "sub": username,
            "permissions": permissions,
            "iat": datetime.utcnow()
        }
        return self.create_access_token(token_data)