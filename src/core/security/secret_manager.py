"""
Secret Management Integration
Provides secure secret management for production deployments.
"""
from __future__ import annotations

import logging
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass

logger = logging.getLogger(__name__)


@dataclass
class SecretConfig:
    """Configuration for secret management."""
    provider: str  # 'vault', 'aws', 'azure', 'gcp', 'env'
    vault_url: str | None = None
    vault_token: str | None = None
    vault_mount_point: str = 'secret'
    aws_region: str | None = None
    azure_vault_url: str | None = None


class SecretProvider(ABC):
    """Abstract base class for secret providers."""

    @abstractmethod
    async def get_secret(self, key: str) -> str | None:
        """Get a secret by key."""
        pass

    @abstractmethod
    async def set_secret(self, key: str, value: str) -> bool:
        """Set a secret."""
        pass

    @abstractmethod
    async def delete_secret(self, key: str) -> bool:
        """Delete a secret."""
        pass


class EnvironmentSecretProvider(SecretProvider):
    """Environment variable secret provider (for development)."""

    async def get_secret(self, key: str) -> str | None:
        """Get secret from environment variable."""
        return os.getenv(key)

    async def set_secret(self, key: str, value: str) -> bool:
        """Cannot set environment variables at runtime."""
        logger.warning("Cannot set environment variables at runtime")
        return False

    async def delete_secret(self, key: str) -> bool:
        """Cannot delete environment variables at runtime."""
        logger.warning("Cannot delete environment variables at runtime")
        return False


class HashiCorpVaultProvider(SecretProvider):
    """HashiCorp Vault secret provider."""

    def __init__(self, config: SecretConfig):
        self.config = config
        self._client = None

    async def _get_client(self):
        """Get or create vault client."""
        if self._client is None:
            try:
                import hvac
                self._client = hvac.Client(
                    url=self.config.vault_url,
                    token=self.config.vault_token
                )
                if not self._client.is_authenticated():
                    raise ValueError("Vault authentication failed")
            except ImportError:
                raise ImportError("hvac package required for Vault integration")
        return self._client

    async def get_secret(self, key: str) -> str | None:
        """Get secret from Vault."""
        try:
            client = await self._get_client()
            response = client.secrets.kv.v2.read_secret_version(
                path=key,
                mount_point=self.config.vault_mount_point
            )
            return response['data']['data'].get('value')
        except Exception as e:
            logger.error(f"Error getting secret from Vault: {e}")
            return None

    async def set_secret(self, key: str, value: str) -> bool:
        """Set secret in Vault."""
        try:
            client = await self._get_client()
            client.secrets.kv.v2.create_or_update_secret(
                path=key,
                secret={'value': value},
                mount_point=self.config.vault_mount_point
            )
            return True
        except Exception as e:
            logger.error(f"Error setting secret in Vault: {e}")
            return False

    async def delete_secret(self, key: str) -> bool:
        """Delete secret from Vault."""
        try:
            client = await self._get_client()
            client.secrets.kv.v2.delete_metadata_and_all_versions(
                path=key,
                mount_point=self.config.vault_mount_point
            )
            return True
        except Exception as e:
            logger.error(f"Error deleting secret from Vault: {e}")
            return False


class AWSSecretsManagerProvider(SecretProvider):
    """AWS Secrets Manager provider."""

    def __init__(self, config: SecretConfig):
        self.config = config
        self._client = None

    async def _get_client(self):
        """Get or create AWS client."""
        if self._client is None:
            try:
                import boto3
                self._client = boto3.client(
                    'secretsmanager',
                    region_name=self.config.aws_region
                )
            except ImportError:
                raise ImportError("boto3 package required for AWS Secrets Manager")
        return self._client

    async def get_secret(self, key: str) -> str | None:
        """Get secret from AWS Secrets Manager."""
        try:
            client = await self._get_client()
            response = client.get_secret_value(SecretId=key)
            return response['SecretString']
        except Exception as e:
            logger.error(f"Error getting secret from AWS: {e}")
            return None

    async def set_secret(self, key: str, value: str) -> bool:
        """Set secret in AWS Secrets Manager."""
        try:
            client = await self._get_client()
            client.create_secret(Name=key, SecretString=value)
            return True
        except client.exceptions.ResourceExistsException:
            # Update existing secret
            client.update_secret(SecretId=key, SecretString=value)
            return True
        except Exception as e:
            logger.error(f"Error setting secret in AWS: {e}")
            return False

    async def delete_secret(self, key: str) -> bool:
        """Delete secret from AWS Secrets Manager."""
        try:
            client = await self._get_client()
            client.delete_secret(SecretId=key, ForceDeleteWithoutRecovery=True)
            return True
        except Exception as e:
            logger.error(f"Error deleting secret from AWS: {e}")
            return False


class SecretManager:
    """
    High-level secret manager that coordinates secret providers.
    Provides caching, fallback, and security features.
    """

    def __init__(self, config: SecretConfig):
        self.config = config
        self._provider = self._create_provider()
        self._cache: dict[str, str] = {}
        self._cache_enabled = True

    def _create_provider(self) -> SecretProvider:
        """Create secret provider based on configuration."""
        if self.config.provider == 'vault':
            return HashiCorpVaultProvider(self.config)
        elif self.config.provider == 'aws':
            return AWSSecretsManagerProvider(self.config)
        elif self.config.provider == 'env':
            return EnvironmentSecretProvider()
        else:
            raise ValueError(f"Unsupported secret provider: {self.config.provider}")

    async def get_secret(self, key: str, use_cache: bool = True) -> str | None:
        """
        Get secret with caching and fallback.
        
        Args:
            key: Secret key
            use_cache: Whether to use cached value
            
        Returns:
            Secret value or None if not found
        """
        # Check cache first
        if use_cache and self._cache_enabled and key in self._cache:
            return self._cache[key]

        # Get from provider
        value = await self._provider.get_secret(key)

        # Cache the value
        if value and self._cache_enabled:
            self._cache[key] = value

        return value

    async def get_required_secret(self, key: str) -> str:
        """Get required secret that must exist."""
        value = await self.get_secret(key)
        if value is None:
            raise ValueError(f"Required secret '{key}' not found")
        return value

    async def set_secret(self, key: str, value: str) -> bool:
        """Set secret and update cache."""
        success = await self._provider.set_secret(key, value)
        if success and self._cache_enabled:
            self._cache[key] = value
        return success

    async def delete_secret(self, key: str) -> bool:
        """Delete secret and remove from cache."""
        success = await self._provider.delete_secret(key)
        if success and key in self._cache:
            del self._cache[key]
        return success

    def clear_cache(self) -> None:
        """Clear secret cache."""
        self._cache.clear()

    def disable_cache(self) -> None:
        """Disable secret caching."""
        self._cache_enabled = False
        self.clear_cache()


# Global secret manager instance
_secret_manager: SecretManager | None = None


def get_secret_manager() -> SecretManager:
    """Get or create global secret manager."""
    global _secret_manager

    if _secret_manager is None:
        # Create from environment configuration
        provider = os.getenv('SECRET_PROVIDER', 'env')

        config = SecretConfig(
            provider=provider,
            vault_url=os.getenv('VAULT_URL'),
            vault_token=os.getenv('VAULT_TOKEN'),
            vault_mount_point=os.getenv('VAULT_MOUNT_POINT', 'secret'),
            aws_region=os.getenv('AWS_REGION', 'us-east-1'),
            azure_vault_url=os.getenv('AZURE_VAULT_URL')
        )

        _secret_manager = SecretManager(config)

    return _secret_manager


async def get_secret(key: str) -> str | None:
    """Convenience function to get a secret."""
    manager = get_secret_manager()
    return await manager.get_secret(key)


async def get_required_secret(key: str) -> str:
    """Convenience function to get a required secret."""
    manager = get_secret_manager()
    return await manager.get_required_secret(key)


# Security utilities
def generate_strong_password(length: int = 32) -> str:
    """Generate a cryptographically strong password."""
    import secrets
    import string

    alphabet = string.ascii_letters + string.digits + "!@#$%^&*"
    return ''.join(secrets.choice(alphabet) for _ in range(length))


def generate_api_key() -> str:
    """Generate a secure API key."""
    import secrets
    return secrets.token_urlsafe(32)


def generate_secret_key() -> str:
    """Generate a secure secret key for JWT/encryption."""
    import secrets
    return secrets.token_urlsafe(64)


# Production secret initialization
async def initialize_production_secrets() -> dict[str, str]:
    """
    Initialize all production secrets with strong values.
    Should be run during deployment setup.
    """
    manager = get_secret_manager()

    secrets_to_generate = {
        'SECRET_KEY': generate_secret_key(),
        'JWT_SECRET_KEY': generate_secret_key(),
        'TYPESENSE_API_KEY': generate_api_key(),
        'BASIC_AUTH_PASSWORD': generate_strong_password(),
        'SPARK_AUTH_SECRET': generate_secret_key(),
        'REDIS_PASSWORD': generate_strong_password(),
        'GRAFANA_ADMIN_PASSWORD': generate_strong_password()
    }

    generated = {}
    for key, value in secrets_to_generate.items():
        success = await manager.set_secret(key, value)
        if success:
            generated[key] = value
            logger.info(f"Generated and stored secret: {key}")
        else:
            logger.error(f"Failed to store secret: {key}")

    return generated
