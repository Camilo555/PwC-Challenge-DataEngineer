"""
Secret Management Integration for Configuration
Integrates secret manager with application configuration system.
"""

import os
import asyncio
from typing import Dict, Any, Optional
from functools import lru_cache

from core.logging import get_logger
from core.security.secret_manager import get_secret_manager

logger = get_logger(__name__)


class SecretConfigMixin:
    """Mixin class to add secret management to configuration classes."""
    
    @classmethod
    def from_secrets(cls, secret_overrides: Optional[Dict[str, str]] = None):
        """
        Create configuration instance using secrets from secret manager.
        
        Args:
            secret_overrides: Optional dictionary to override specific secrets
        """
        try:
            # Get secrets synchronously using asyncio
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            secrets = loop.run_until_complete(_load_secrets_async(secret_overrides))
            loop.close()
            
            # Merge with environment variables (env vars take precedence)
            config_data = {**secrets}
            for key, value in os.environ.items():
                if value:  # Only override if env var has a value
                    config_data[key] = value
            
            # Temporarily set environment variables for pydantic
            original_env = {}
            for key, value in config_data.items():
                if key not in os.environ:
                    original_env[key] = os.environ.get(key)
                    os.environ[key] = value
            
            try:
                instance = cls()
            finally:
                # Restore original environment
                for key, original_value in original_env.items():
                    if original_value is None:
                        os.environ.pop(key, None)
                    else:
                        os.environ[key] = original_value
            
            return instance
            
        except Exception as e:
            logger.warning(f"Failed to load secrets, falling back to environment variables: {e}")
            return cls()


async def _load_secrets_async(secret_overrides: Optional[Dict[str, str]] = None) -> Dict[str, str]:
    """Load secrets asynchronously from secret manager."""
    secret_manager = get_secret_manager()
    secrets = {}
    
    # Common secret keys that configuration classes might need
    secret_keys = [
        'SECRET_KEY',
        'JWT_SECRET_KEY', 
        'DATABASE_URL',
        'BASIC_AUTH_PASSWORD',
        'BASIC_AUTH_USERNAME',
        'ADMIN_USERNAME',
        'TYPESENSE_API_KEY',
        'SPARK_AUTH_SECRET',
        'REDIS_PASSWORD',
        'GRAFANA_ADMIN_PASSWORD',
        'SMTP_PASSWORD',
        'AWS_ACCESS_KEY_ID',
        'AWS_SECRET_ACCESS_KEY',
        'VAULT_TOKEN',
        'VAULT_URL',
        'OAUTH2_CLIENT_ID',
        'OAUTH2_CLIENT_SECRET'
    ]
    
    # Apply overrides if provided
    if secret_overrides:
        secret_keys.extend(secret_overrides.keys())
    
    # Load secrets
    for key in secret_keys:
        try:
            if secret_overrides and key in secret_overrides:
                value = secret_overrides[key]
            else:
                value = await secret_manager.get_secret(key)
            
            if value:
                secrets[key] = value
                
        except Exception as e:
            logger.debug(f"Could not load secret '{key}': {e}")
    
    return secrets


@lru_cache(maxsize=1)
def get_database_url_from_secrets() -> Optional[str]:
    """Get database URL from secret manager with caching."""
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        secret_manager = get_secret_manager()
        database_url = loop.run_until_complete(secret_manager.get_secret('DATABASE_URL'))
        loop.close()
        return database_url
    except Exception as e:
        logger.debug(f"Could not get database URL from secrets: {e}")
        return None


@lru_cache(maxsize=1) 
def get_jwt_secret_from_secrets() -> Optional[str]:
    """Get JWT secret from secret manager with caching."""
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        secret_manager = get_secret_manager()
        jwt_secret = loop.run_until_complete(secret_manager.get_secret('JWT_SECRET_KEY'))
        loop.close()
        return jwt_secret
    except Exception as e:
        logger.debug(f"Could not get JWT secret from secrets: {e}")
        return None


def ensure_secrets_initialized():
    """Ensure all required secrets are initialized before application startup."""
    try:
        from core.security.secret_initialization import get_secret_initializer
        
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        initializer = get_secret_initializer()
        health_check = loop.run_until_complete(initializer.health_check())
        
        if health_check['status'] == 'unhealthy':
            logger.warning("Required secrets are missing, initializing...")
            loop.run_until_complete(initializer.initialize_secrets())
            logger.info("✅ Secrets initialized successfully")
        elif health_check['status'] == 'degraded':
            logger.info("⚠️ Some optional secrets are missing but system is functional")
        else:
            logger.info("✅ All secrets are properly configured")
        
        loop.close()
        
    except Exception as e:
        logger.error(f"Secret initialization failed: {e}")
        # Don't raise - allow application to start with environment variables


# Startup hook
def initialize_secrets_on_startup():
    """Hook to initialize secrets during application startup."""
    import os
    
    # Only initialize secrets in production or when explicitly requested
    environment = os.getenv('ENVIRONMENT', 'development').lower()
    force_init = os.getenv('FORCE_SECRET_INIT', '').lower() in ('true', '1', 'yes')
    
    if environment == 'production' or force_init:
        logger.info("Initializing secret management for production environment...")
        ensure_secrets_initialized()
    else:
        logger.debug("Skipping secret initialization in development environment")


# Export convenience functions
__all__ = [
    'SecretConfigMixin',
    'get_database_url_from_secrets',
    'get_jwt_secret_from_secrets', 
    'ensure_secrets_initialized',
    'initialize_secrets_on_startup'
]