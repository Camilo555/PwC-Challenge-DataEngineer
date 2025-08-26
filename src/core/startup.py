"""
Application Startup Module
Handles initialization of critical systems including secret management.
"""
from __future__ import annotations

import asyncio
from typing import Any

from core.config.secret_integration import initialize_secrets_on_startup
from core.logging import get_logger

logger = get_logger(__name__)


class ApplicationStartup:
    """Manages application startup procedures."""

    def __init__(self):
        self.startup_checks = []
        self.startup_tasks = []
        self.initialized = False

    async def startup(self, skip_secret_init: bool = False) -> dict[str, Any]:
        """
        Perform complete application startup.
        
        Args:
            skip_secret_init: Skip secret initialization (for testing)
            
        Returns:
            Dictionary with startup results
        """
        logger.info("🚀 Starting application initialization...")

        startup_results = {
            'status': 'success',
            'components': {},
            'errors': [],
            'warnings': []
        }

        try:
            # 1. Initialize secret management
            if not skip_secret_init:
                await self._initialize_secrets(startup_results)

            # 2. Validate configuration
            await self._validate_configuration(startup_results)

            # 3. Initialize database connections
            await self._initialize_database(startup_results)

            # 4. Initialize distributed tracing
            await self._initialize_tracing(startup_results)

            # 5. Initialize monitoring systems
            await self._initialize_monitoring(startup_results)

            # 6. Run health checks
            await self._run_health_checks(startup_results)

            # 7. Final validation
            await self._final_validation(startup_results)

            self.initialized = True
            logger.info("✅ Application startup completed successfully")

        except Exception as e:
            startup_results['status'] = 'error'
            startup_results['errors'].append(f"Startup failed: {str(e)}")
            logger.error(f"❌ Application startup failed: {e}")
            raise

        return startup_results

    async def _initialize_secrets(self, results: dict[str, Any]) -> None:
        """Initialize secret management system."""
        logger.info("🔐 Initializing secret management...")

        try:
            # Initialize secrets synchronously for startup
            initialize_secrets_on_startup()

            # Verify secret health
            from core.security.secret_initialization import perform_secret_health_check
            health_check = await perform_secret_health_check()

            results['components']['secret_management'] = {
                'status': health_check['status'],
                'provider': health_check['provider'],
                'secret_counts': health_check['secret_counts']
            }

            if health_check['status'] == 'unhealthy':
                results['errors'].extend(health_check['issues'])
            elif health_check['status'] == 'degraded':
                results['warnings'].extend(health_check['issues'])

            logger.info(f"Secret management status: {health_check['status']}")

        except Exception as e:
            error_msg = f"Secret management initialization failed: {e}"
            results['errors'].append(error_msg)
            logger.error(error_msg)

    async def _validate_configuration(self, results: dict[str, Any]) -> None:
        """Validate application configuration."""
        logger.info("⚙️ Validating configuration...")

        try:
            from core.config import settings

            # Validate paths
            settings.validate_paths()

            # Check environment
            environment = settings.environment

            results['components']['configuration'] = {
                'status': 'healthy',
                'environment': environment.value,
                'processing_engine': settings.processing_engine.value,
                'orchestration_engine': settings.orchestration_engine.value
            }

            logger.info(f"Configuration validated for environment: {environment.value}")

        except Exception as e:
            error_msg = f"Configuration validation failed: {e}"
            results['errors'].append(error_msg)
            logger.error(error_msg)

    async def _initialize_database(self, results: dict[str, Any]) -> None:
        """Initialize database connections."""
        logger.info("🗄️ Initializing database connections...")

        try:
            from core.config import settings

            # Test database connection
            database_url = settings.get_database_url()

            if database_url:
                results['components']['database'] = {
                    'status': 'healthy',
                    'type': 'sqlite' if 'sqlite' in database_url else 'postgresql',
                    'url_configured': True
                }
            else:
                results['warnings'].append("Database URL not configured")
                results['components']['database'] = {
                    'status': 'degraded',
                    'url_configured': False
                }

            logger.info("Database configuration validated")

        except Exception as e:
            error_msg = f"Database initialization failed: {e}"
            results['errors'].append(error_msg)
            logger.error(error_msg)

    async def _initialize_tracing(self, results: dict[str, Any]) -> None:
        """Initialize distributed tracing system."""
        logger.info("🔍 Initializing distributed tracing...")

        try:
            from core.tracing.startup import setup_tracing_for_startup

            tracing_results = await setup_tracing_for_startup()

            results['components']['tracing'] = {
                'status': 'healthy' if tracing_results.get('tracing_enabled') else 'disabled',
                'enabled': tracing_results.get('tracing_enabled', False),
                'backend': tracing_results.get('configuration', {}).get('backend', 'none'),
                'service_name': tracing_results.get('configuration', {}).get('service_name', 'unknown')
            }

            if tracing_results.get('errors'):
                results['warnings'].extend([f"Tracing: {error}" for error in tracing_results['errors']])

            # Log instrumentation results
            instrumentation = tracing_results.get('instrumentation', {})
            if instrumentation:
                successful = sum(1 for r in instrumentation.values() if r)
                total = len(instrumentation)
                results['components']['tracing']['instrumentation'] = f"{successful}/{total} libraries"

            logger.info(f"Tracing initialization: {tracing_results.get('status', 'unknown')}")

        except Exception as e:
            error_msg = f"Tracing initialization failed: {e}"
            results['warnings'].append(error_msg)
            logger.warning(error_msg)

    async def _initialize_monitoring(self, results: dict[str, Any]) -> None:
        """Initialize monitoring systems."""
        logger.info("📊 Initializing monitoring systems...")

        try:
            from core.config import settings

            monitoring_enabled = settings.enable_monitoring

            results['components']['monitoring'] = {
                'status': 'healthy' if monitoring_enabled else 'disabled',
                'enabled': monitoring_enabled
            }

            if monitoring_enabled:
                # Initialize metrics collectors
                try:
                    from monitoring.metrics_collector import MetricCollector
                    collector = MetricCollector()
                    results['components']['monitoring']['metrics_collector'] = 'initialized'
                except ImportError:
                    results['warnings'].append("Monitoring modules not available")

            logger.info(f"Monitoring initialization: {'enabled' if monitoring_enabled else 'disabled'}")

        except Exception as e:
            error_msg = f"Monitoring initialization failed: {e}"
            results['warnings'].append(error_msg)
            logger.warning(error_msg)

    async def _run_health_checks(self, results: dict[str, Any]) -> None:
        """Run comprehensive health checks."""
        logger.info("🏥 Running health checks...")

        try:
            health_results = {
                'overall_status': 'healthy',
                'checks': {}
            }

            # File system health check
            from core.config import settings
            try:
                settings.validate_paths()
                health_results['checks']['filesystem'] = 'healthy'
            except Exception as e:
                health_results['checks']['filesystem'] = f'unhealthy: {e}'
                health_results['overall_status'] = 'degraded'

            # Memory health check
            import psutil
            memory_percent = psutil.virtual_memory().percent
            if memory_percent > 90:
                health_results['checks']['memory'] = f'warning: {memory_percent}% used'
                health_results['overall_status'] = 'degraded'
            else:
                health_results['checks']['memory'] = f'healthy: {memory_percent}% used'

            # Disk space health check
            disk_usage = psutil.disk_usage('/')
            disk_percent = (disk_usage.used / disk_usage.total) * 100
            if disk_percent > 90:
                health_results['checks']['disk'] = f'warning: {disk_percent:.1f}% used'
                health_results['overall_status'] = 'degraded'
            else:
                health_results['checks']['disk'] = f'healthy: {disk_percent:.1f}% used'

            results['components']['health_checks'] = health_results
            logger.info(f"Health checks completed: {health_results['overall_status']}")

        except Exception as e:
            error_msg = f"Health checks failed: {e}"
            results['warnings'].append(error_msg)
            logger.warning(error_msg)

    async def _final_validation(self, results: dict[str, Any]) -> None:
        """Perform final startup validation."""
        logger.info("✅ Performing final validation...")

        # Determine overall status
        if results['errors']:
            results['status'] = 'error'
            logger.error(f"Startup completed with {len(results['errors'])} errors")
        elif results['warnings']:
            results['status'] = 'warning'
            logger.warning(f"Startup completed with {len(results['warnings'])} warnings")
        else:
            results['status'] = 'success'
            logger.info("Startup completed successfully")

        # Log summary
        component_count = len(results['components'])
        healthy_components = sum(1 for comp in results['components'].values()
                               if comp.get('status') == 'healthy')

        logger.info(f"Startup summary: {healthy_components}/{component_count} components healthy")


# Global startup manager
_startup_manager: ApplicationStartup | None = None


def get_startup_manager() -> ApplicationStartup:
    """Get or create global startup manager."""
    global _startup_manager

    if _startup_manager is None:
        _startup_manager = ApplicationStartup()

    return _startup_manager


async def startup_application(skip_secret_init: bool = False) -> dict[str, Any]:
    """Convenience function to startup the application."""
    manager = get_startup_manager()
    return await manager.startup(skip_secret_init=skip_secret_init)


def is_application_initialized() -> bool:
    """Check if the application has been initialized."""
    global _startup_manager
    return _startup_manager is not None and _startup_manager.initialized


# CLI support
async def cli_startup_check() -> None:
    """CLI command to check startup status."""
    try:
        results = await startup_application()

        status_emoji = {
            'success': '✅',
            'warning': '⚠️',
            'error': '❌'
        }

        print(f"{status_emoji.get(results['status'], '❓')} Startup Status: {results['status'].upper()}")

        print(f"\nComponents ({len(results['components'])}):")
        for name, component in results['components'].items():
            status = component.get('status', 'unknown')
            comp_emoji = status_emoji.get(status, '❓')
            print(f"  {comp_emoji} {name}: {status}")

        if results['warnings']:
            print(f"\nWarnings ({len(results['warnings'])}):")
            for warning in results['warnings']:
                print(f"  ⚠️ {warning}")

        if results['errors']:
            print(f"\nErrors ({len(results['errors'])}):")
            for error in results['errors']:
                print(f"  ❌ {error}")

    except Exception as e:
        print(f"💥 Startup check failed: {e}")
        raise


if __name__ == "__main__":
    asyncio.run(cli_startup_check())
