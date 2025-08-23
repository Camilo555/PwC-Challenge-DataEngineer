"""
Connection Pool Configuration

Optimized connection pool configurations for different environments and database types.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Any

from core.config.base_config import DatabaseType, Environment


class PoolStrategy(str, Enum):
    """Connection pooling strategies."""
    CONSERVATIVE = "conservative"  # Low resource usage
    BALANCED = "balanced"          # Balanced performance/resources
    AGGRESSIVE = "aggressive"      # Maximum performance
    AUTO = "auto"                  # Environment-based selection


@dataclass
class ConnectionPoolConfig:
    """
    Advanced connection pool configuration with environment-aware defaults.
    
    This configuration is optimized for different environments and workloads:
    - Development: Low resource usage, fast iteration
    - Testing: Isolated connections, predictable behavior
    - Staging: Production-like settings with monitoring
    - Production: High performance, reliability, monitoring
    """

    # Basic pool settings
    pool_size: int = 10
    max_overflow: int = 20
    pool_timeout: int = 30
    pool_recycle: int = 3600  # 1 hour
    pool_pre_ping: bool = True

    # Connection settings
    connect_timeout: int = 30
    connect_retries: int = 3
    retry_delay: float = 1.0

    # Health and monitoring
    health_check_interval: int = 300  # 5 minutes
    max_connection_age: int = 3600    # 1 hour
    enable_events: bool = True
    enable_metrics: bool = True

    # Performance tuning
    echo: bool = False
    echo_pool: bool = False
    isolation_level: str | None = None

    # Async-specific settings
    async_pool_size: int = 5
    async_max_overflow: int = 10

    @classmethod
    def for_environment(cls, environment: Environment, db_type: DatabaseType = DatabaseType.SQLITE) -> 'ConnectionPoolConfig':
        """Create optimized pool configuration for specific environment."""
        configs = {
            Environment.DEVELOPMENT: cls._get_development_config(db_type),
            Environment.TESTING: cls._get_testing_config(db_type),
            Environment.STAGING: cls._get_staging_config(db_type),
            Environment.PRODUCTION: cls._get_production_config(db_type),
        }

        return configs.get(environment, cls._get_development_config(db_type))

    @classmethod
    def for_strategy(cls, strategy: PoolStrategy, db_type: DatabaseType = DatabaseType.SQLITE) -> 'ConnectionPoolConfig':
        """Create configuration based on pooling strategy."""
        strategies = {
            PoolStrategy.CONSERVATIVE: cls._get_conservative_config(db_type),
            PoolStrategy.BALANCED: cls._get_balanced_config(db_type),
            PoolStrategy.AGGRESSIVE: cls._get_aggressive_config(db_type),
            PoolStrategy.AUTO: cls._get_balanced_config(db_type),  # Default to balanced
        }

        return strategies.get(strategy, cls._get_balanced_config(db_type))

    @classmethod
    def _get_development_config(cls, db_type: DatabaseType) -> 'ConnectionPoolConfig':
        """Optimized for development - low resource usage, fast iteration."""
        config = cls(
            # Small pool for development
            pool_size=3,
            max_overflow=5,
            pool_timeout=10,
            pool_recycle=1800,  # 30 minutes
            pool_pre_ping=True,

            # Fast connection setup
            connect_timeout=10,
            connect_retries=2,
            retry_delay=0.5,

            # Less frequent health checks
            health_check_interval=600,  # 10 minutes
            max_connection_age=1800,    # 30 minutes

            # Development debugging
            echo=False,  # Can be enabled for debugging
            echo_pool=False,

            # Smaller async pool
            async_pool_size=2,
            async_max_overflow=3,
        )

        if db_type == DatabaseType.SQLITE:
            # SQLite-specific optimizations for development
            config.pool_size = 1
            config.max_overflow = 2
            config.isolation_level = None

        return config

    @classmethod
    def _get_testing_config(cls, db_type: DatabaseType) -> 'ConnectionPoolConfig':
        """Optimized for testing - isolated, predictable behavior."""
        config = cls(
            # Small, isolated pool
            pool_size=2,
            max_overflow=2,
            pool_timeout=5,
            pool_recycle=300,   # 5 minutes
            pool_pre_ping=True,

            # Fast timeouts for testing
            connect_timeout=5,
            connect_retries=1,
            retry_delay=0.1,

            # Minimal health checking
            health_check_interval=0,  # Disabled
            max_connection_age=300,   # 5 minutes
            enable_events=False,      # Minimal overhead
            enable_metrics=False,

            # No debugging in tests
            echo=False,
            echo_pool=False,

            # Minimal async pool
            async_pool_size=1,
            async_max_overflow=1,
        )

        if db_type == DatabaseType.SQLITE:
            # SQLite testing optimizations
            config.pool_size = 1
            config.max_overflow = 0
            config.isolation_level = None

        return config

    @classmethod
    def _get_staging_config(cls, db_type: DatabaseType) -> 'ConnectionPoolConfig':
        """Optimized for staging - production-like with monitoring."""
        config = cls(
            # Production-like pool sizes
            pool_size=8,
            max_overflow=15,
            pool_timeout=20,
            pool_recycle=2700,  # 45 minutes
            pool_pre_ping=True,

            # Moderate connection settings
            connect_timeout=20,
            connect_retries=3,
            retry_delay=1.0,

            # Active health monitoring
            health_check_interval=300,  # 5 minutes
            max_connection_age=2700,    # 45 minutes
            enable_events=True,
            enable_metrics=True,

            # No debugging in staging
            echo=False,
            echo_pool=False,

            # Moderate async pool
            async_pool_size=4,
            async_max_overflow=8,
        )

        if db_type == DatabaseType.POSTGRESQL:
            config.isolation_level = "READ_COMMITTED"

        return config

    @classmethod
    def _get_production_config(cls, db_type: DatabaseType) -> 'ConnectionPoolConfig':
        """Optimized for production - maximum performance and reliability."""
        config = cls(
            # Large pool for high concurrency
            pool_size=20,
            max_overflow=40,
            pool_timeout=30,
            pool_recycle=3600,  # 1 hour
            pool_pre_ping=True,

            # Robust connection handling
            connect_timeout=30,
            connect_retries=5,
            retry_delay=2.0,

            # Comprehensive health monitoring
            health_check_interval=180,  # 3 minutes
            max_connection_age=3600,    # 1 hour
            enable_events=True,
            enable_metrics=True,

            # No debugging in production
            echo=False,
            echo_pool=False,

            # Large async pool
            async_pool_size=10,
            async_max_overflow=20,
        )

        if db_type == DatabaseType.POSTGRESQL:
            config.isolation_level = "READ_COMMITTED"
        elif db_type == DatabaseType.SQLITE:
            # SQLite production optimizations (though not recommended for high-load production)
            config.pool_size = 3
            config.max_overflow = 7
            config.isolation_level = None

        return config

    @classmethod
    def _get_conservative_config(cls, db_type: DatabaseType) -> 'ConnectionPoolConfig':
        """Conservative strategy - minimal resource usage."""
        return cls(
            pool_size=3,
            max_overflow=5,
            pool_timeout=10,
            pool_recycle=1800,
            async_pool_size=2,
            async_max_overflow=3,
            health_check_interval=600,
        )

    @classmethod
    def _get_balanced_config(cls, db_type: DatabaseType) -> 'ConnectionPoolConfig':
        """Balanced strategy - good performance with reasonable resource usage."""
        config = cls(
            pool_size=10,
            max_overflow=20,
            pool_timeout=30,
            pool_recycle=3600,
            async_pool_size=5,
            async_max_overflow=10,
            health_check_interval=300,
        )

        if db_type == DatabaseType.POSTGRESQL:
            config.isolation_level = "READ_COMMITTED"

        return config

    @classmethod
    def _get_aggressive_config(cls, db_type: DatabaseType) -> 'ConnectionPoolConfig':
        """Aggressive strategy - maximum performance."""
        config = cls(
            pool_size=25,
            max_overflow=50,
            pool_timeout=45,
            pool_recycle=7200,  # 2 hours
            async_pool_size=15,
            async_max_overflow=25,
            health_check_interval=120,  # 2 minutes
            connect_retries=5,
        )

        if db_type == DatabaseType.POSTGRESQL:
            config.isolation_level = "READ_COMMITTED"
        elif db_type == DatabaseType.SQLITE:
            # SQLite can't handle aggressive pooling well
            config.pool_size = 5
            config.max_overflow = 10

        return config

    def get_sqlalchemy_pool_kwargs(self) -> dict[str, Any]:
        """Get SQLAlchemy connection pool keyword arguments."""
        return {
            'pool_size': self.pool_size,
            'max_overflow': self.max_overflow,
            'pool_timeout': self.pool_timeout,
            'pool_recycle': self.pool_recycle,
            'pool_pre_ping': self.pool_pre_ping,
            'echo': self.echo,
            'echo_pool': self.echo_pool,
        }

    def get_async_pool_kwargs(self) -> dict[str, Any]:
        """Get async SQLAlchemy connection pool keyword arguments."""
        return {
            'pool_size': self.async_pool_size,
            'max_overflow': self.async_max_overflow,
            'pool_timeout': self.pool_timeout,
            'pool_recycle': self.pool_recycle,
            'pool_pre_ping': self.pool_pre_ping,
            'echo': self.echo,
        }

    def get_connection_kwargs(self, db_type: DatabaseType) -> dict[str, Any]:
        """Get database-specific connection arguments."""
        if db_type == DatabaseType.SQLITE:
            return {
                'check_same_thread': False,
                'timeout': self.connect_timeout,
                'isolation_level': self.isolation_level,
            }
        elif db_type == DatabaseType.POSTGRESQL:
            return {
                'connect_timeout': self.connect_timeout,
                'command_timeout': self.pool_timeout,
            }

        return {}

    def to_dict(self) -> dict[str, Any]:
        """Convert configuration to dictionary."""
        return {
            'pool_size': self.pool_size,
            'max_overflow': self.max_overflow,
            'pool_timeout': self.pool_timeout,
            'pool_recycle': self.pool_recycle,
            'pool_pre_ping': self.pool_pre_ping,
            'connect_timeout': self.connect_timeout,
            'connect_retries': self.connect_retries,
            'retry_delay': self.retry_delay,
            'health_check_interval': self.health_check_interval,
            'max_connection_age': self.max_connection_age,
            'enable_events': self.enable_events,
            'enable_metrics': self.enable_metrics,
            'async_pool_size': self.async_pool_size,
            'async_max_overflow': self.async_max_overflow,
        }
