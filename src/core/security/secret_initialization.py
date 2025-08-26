"""
Secret Management Initialization
Provides automated secret setup and initialization for production deployments.
"""
from __future__ import annotations

import os

from core.logging import get_logger

from .secret_manager import (
    get_secret_manager,
    initialize_production_secrets,
)

logger = get_logger(__name__)


class SecretInitializer:
    """Handles secret initialization and validation."""

    def __init__(self):
        self.manager = get_secret_manager()
        self.required_secrets = [
            'SECRET_KEY',
            'JWT_SECRET_KEY',
            'DATABASE_URL',
            'BASIC_AUTH_PASSWORD'
        ]
        self.optional_secrets = [
            'TYPESENSE_API_KEY',
            'SPARK_AUTH_SECRET',
            'REDIS_PASSWORD',
            'GRAFANA_ADMIN_PASSWORD',
            'SMTP_PASSWORD',
            'AWS_ACCESS_KEY_ID',
            'AWS_SECRET_ACCESS_KEY'
        ]

    async def initialize_secrets(self, force_regenerate: bool = False) -> dict[str, str]:
        """
        Initialize all required secrets.
        
        Args:
            force_regenerate: Whether to regenerate existing secrets
            
        Returns:
            Dictionary of initialized secrets (values masked for security)
        """
        logger.info("Starting secret initialization...")

        # Check which secrets are missing
        missing_secrets = await self._check_missing_secrets()

        if missing_secrets or force_regenerate:
            logger.info(f"Initializing {len(missing_secrets)} missing secrets...")
            generated_secrets = await initialize_production_secrets()

            # Validate all required secrets are now available
            await self._validate_required_secrets()

            # Return masked results for logging
            return {key: self._mask_secret(value) for key, value in generated_secrets.items()}
        else:
            logger.info("All required secrets are already initialized")
            return {}

    async def _check_missing_secrets(self) -> list[str]:
        """Check which required secrets are missing."""
        missing = []

        for secret_key in self.required_secrets:
            value = await self.manager.get_secret(secret_key)
            if not value:
                missing.append(secret_key)

        return missing

    async def _validate_required_secrets(self) -> None:
        """Validate that all required secrets are available."""
        missing_secrets = await self._check_missing_secrets()

        if missing_secrets:
            raise ValueError(f"Required secrets are still missing: {missing_secrets}")

        logger.info("All required secrets are properly initialized")

    async def rotate_secrets(self, secret_keys: list[str] | None = None) -> dict[str, str]:
        """
        Rotate (regenerate) specified secrets.
        
        Args:
            secret_keys: List of secrets to rotate, or None for all secrets
            
        Returns:
            Dictionary of rotated secrets (values masked)
        """
        if secret_keys is None:
            secret_keys = self.required_secrets + self.optional_secrets

        logger.info(f"Rotating {len(secret_keys)} secrets...")

        rotated = {}
        for secret_key in secret_keys:
            # Generate new secret value
            from core.security.secret_manager import (
                generate_api_key,
                generate_secret_key,
                generate_strong_password,
            )

            if 'SECRET_KEY' in secret_key or 'JWT' in secret_key:
                new_value = generate_secret_key()
            elif 'API_KEY' in secret_key:
                new_value = generate_api_key()
            else:
                new_value = generate_strong_password()

            # Store the new secret
            success = await self.manager.set_secret(secret_key, new_value)
            if success:
                rotated[secret_key] = self._mask_secret(new_value)
                logger.info(f"Successfully rotated secret: {secret_key}")
            else:
                logger.error(f"Failed to rotate secret: {secret_key}")

        return rotated

    async def export_secrets_for_deployment(self, output_format: str = 'env') -> str:
        """
        Export secrets in deployment-ready format.
        
        Args:
            output_format: Format to export ('env', 'json', 'yaml')
            
        Returns:
            Formatted secret export string
        """
        logger.info(f"Exporting secrets in {output_format} format...")

        secrets = {}
        all_secret_keys = self.required_secrets + self.optional_secrets

        for secret_key in all_secret_keys:
            value = await self.manager.get_secret(secret_key)
            if value:
                secrets[secret_key] = value

        if output_format == 'env':
            return self._format_as_env_file(secrets)
        elif output_format == 'json':
            import json
            return json.dumps(secrets, indent=2)
        elif output_format == 'yaml':
            import yaml
            return yaml.dump(secrets, default_flow_style=False)
        else:
            raise ValueError(f"Unsupported output format: {output_format}")

    def _format_as_env_file(self, secrets: dict[str, str]) -> str:
        """Format secrets as .env file content."""
        lines = ["# Auto-generated secrets - DO NOT COMMIT TO VERSION CONTROL"]
        lines.append(f"# Generated at: {os.environ.get('TIMESTAMP', 'unknown')}")
        lines.append("")

        for key, value in sorted(secrets.items()):
            lines.append(f"{key}={value}")

        return "\n".join(lines)

    def _mask_secret(self, value: str) -> str:
        """Mask secret value for safe logging."""
        if len(value) <= 8:
            return "*" * len(value)
        return value[:4] + "*" * (len(value) - 8) + value[-4:]

    async def health_check(self) -> dict[str, any]:
        """Perform health check on secret management system."""
        logger.info("Performing secret management health check...")

        results = {
            'provider': self.manager.config.provider,
            'status': 'healthy',
            'issues': [],
            'secret_counts': {
                'required': 0,
                'optional': 0,
                'missing_required': 0,
                'missing_optional': 0
            }
        }

        try:
            # Check required secrets
            for secret_key in self.required_secrets:
                value = await self.manager.get_secret(secret_key)
                if value:
                    results['secret_counts']['required'] += 1
                else:
                    results['secret_counts']['missing_required'] += 1
                    results['issues'].append(f"Missing required secret: {secret_key}")

            # Check optional secrets
            for secret_key in self.optional_secrets:
                value = await self.manager.get_secret(secret_key)
                if value:
                    results['secret_counts']['optional'] += 1
                else:
                    results['secret_counts']['missing_optional'] += 1

            # Determine overall status
            if results['secret_counts']['missing_required'] > 0:
                results['status'] = 'unhealthy'
            elif results['secret_counts']['missing_optional'] > 0:
                results['status'] = 'degraded'

        except Exception as e:
            results['status'] = 'error'
            results['issues'].append(f"Health check failed: {str(e)}")

        logger.info(f"Secret management health check completed: {results['status']}")
        return results


# Global initializer instance
_secret_initializer: SecretInitializer | None = None


def get_secret_initializer() -> SecretInitializer:
    """Get or create global secret initializer."""
    global _secret_initializer

    if _secret_initializer is None:
        _secret_initializer = SecretInitializer()

    return _secret_initializer


async def initialize_application_secrets() -> dict[str, str]:
    """Convenience function to initialize application secrets."""
    initializer = get_secret_initializer()
    return await initializer.initialize_secrets()


async def perform_secret_health_check() -> dict[str, any]:
    """Convenience function to perform secret health check."""
    initializer = get_secret_initializer()
    return await initializer.health_check()


# CLI command support
async def cli_init_secrets(force: bool = False) -> None:
    """CLI command to initialize secrets."""
    try:
        initializer = get_secret_initializer()
        result = await initializer.initialize_secrets(force_regenerate=force)

        if result:
            print(f"[OK] Successfully initialized {len(result)} secrets:")
            for key, masked_value in result.items():
                print(f"  {key}: {masked_value}")
        else:
            print("[OK] All secrets already initialized")

    except Exception as e:
        print(f"[ERROR] Secret initialization failed: {e}")
        raise


async def cli_rotate_secrets(secret_names: list[str] | None = None) -> None:
    """CLI command to rotate secrets."""
    try:
        initializer = get_secret_initializer()
        result = await initializer.rotate_secrets(secret_names)

        print(f"[OK] Successfully rotated {len(result)} secrets:")
        for key, masked_value in result.items():
            print(f"  {key}: {masked_value}")

    except Exception as e:
        print(f"[ERROR] Secret rotation failed: {e}")
        raise


async def cli_health_check() -> None:
    """CLI command for secret health check."""
    try:
        result = await perform_secret_health_check()

        status_symbols = {
            'healthy': '[OK]',
            'degraded': '[WARN]',
            'unhealthy': '[ERROR]',
            'error': '[FAIL]'
        }

        print(f"{status_symbols.get(result['status'], '[?]')} Secret Management Status: {result['status'].upper()}")
        print(f"Provider: {result['provider']}")
        print(f"Required secrets: {result['secret_counts']['required']}/{len(get_secret_initializer().required_secrets)}")
        print(f"Optional secrets: {result['secret_counts']['optional']}/{len(get_secret_initializer().optional_secrets)}")

        if result['issues']:
            print("\nIssues detected:")
            for issue in result['issues']:
                print(f"  - {issue}")

    except Exception as e:
        print(f"[FAIL] Health check failed: {e}")
        raise
