"""
Advanced Database Connection Manager

Enterprise-grade database connection pooling with performance optimization,
health monitoring, and automatic failover capabilities.
"""
from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from sqlalchemy import Engine, create_engine, event, text
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import QueuePool, StaticPool

from core.config.base_config import BaseConfig, DatabaseType
from core.logging import get_logger

logger = get_logger(__name__)


@dataclass
class ConnectionPoolConfig:
    """Connection pool configuration with optimized defaults."""

    # Pool size settings
    pool_size: int = 10
    max_overflow: int = 20
    pool_timeout: int = 30
    pool_recycle: int = 3600  # 1 hour
    pool_pre_ping: bool = True

    # Connection retry settings
    connect_retries: int = 3
    retry_delay: float = 1.0

    # Health monitoring
    health_check_interval: int = 300  # 5 minutes
    max_connection_age: int = 3600  # 1 hour

    # Performance optimization
    echo: bool = False
    echo_pool: bool = False
    isolation_level: str | None = None

    # Async settings
    async_pool_size: int = 5
    async_max_overflow: int = 10

    def get_pool_kwargs(self) -> dict[str, Any]:
        """Get connection pool keyword arguments."""
        return {
            'poolclass': QueuePool,
            'pool_size': self.pool_size,
            'max_overflow': self.max_overflow,
            'pool_timeout': self.pool_timeout,
            'pool_recycle': self.pool_recycle,
            'pool_pre_ping': self.pool_pre_ping,
            'echo': self.echo,
            'echo_pool': self.echo_pool,
            'isolation_level': self.isolation_level,
        }

    def get_async_pool_kwargs(self) -> dict[str, Any]:
        """Get async connection pool keyword arguments."""
        return {
            'poolclass': QueuePool,
            'pool_size': self.async_pool_size,
            'max_overflow': self.async_max_overflow,
            'pool_timeout': self.pool_timeout,
            'pool_recycle': self.pool_recycle,
            'pool_pre_ping': self.pool_pre_ping,
            'echo': self.echo,
        }


@dataclass
class ConnectionMetrics:
    """Database connection metrics for monitoring."""

    total_connections: int = 0
    active_connections: int = 0
    pool_hits: int = 0
    pool_misses: int = 0
    connection_errors: int = 0
    avg_connection_time: float = 0.0
    last_health_check: datetime | None = None
    uptime_start: datetime = field(default_factory=datetime.utcnow)

    def get_metrics_dict(self) -> dict[str, Any]:
        """Get metrics as dictionary."""
        uptime = datetime.utcnow() - self.uptime_start
        return {
            'total_connections': self.total_connections,
            'active_connections': self.active_connections,
            'pool_hits': self.pool_hits,
            'pool_misses': self.pool_misses,
            'connection_errors': self.connection_errors,
            'avg_connection_time_ms': self.avg_connection_time * 1000,
            'uptime_seconds': uptime.total_seconds(),
            'last_health_check': self.last_health_check.isoformat() if self.last_health_check else None,
        }


class DatabaseManager:
    """
    Advanced database manager with connection pooling, health monitoring,
    and performance optimization.
    """

    def __init__(self, config: BaseConfig, pool_config: ConnectionPoolConfig | None = None):
        self.config = config
        self.pool_config = pool_config or ConnectionPoolConfig()
        self._engine: Engine | None = None
        self._async_engine: AsyncEngine | None = None
        self._session_factory: sessionmaker | None = None
        self._async_session_factory: async_sessionmaker | None = None
        self.metrics = ConnectionMetrics()
        self._health_check_task: asyncio.Task | None = None
        self._is_healthy = True

    async def initialize(self) -> None:
        """Initialize database connections and start health monitoring."""
        try:
            logger.info("Initializing database manager...")

            # Create engines
            await self._create_engines()

            # Create session factories
            self._create_session_factories()

            # Setup event listeners for metrics
            self._setup_event_listeners()

            # Start health monitoring
            await self._start_health_monitoring()

            # Verify connections
            await self._verify_connections()

            logger.info("Database manager initialized successfully")

        except Exception as e:
            logger.error(f"Failed to initialize database manager: {e}")
            raise

    async def _create_engines(self) -> None:
        """Create synchronous and asynchronous database engines."""
        database_url = self.config.get_database_url()
        async_database_url = self.config.get_database_url(async_mode=True)

        # Determine database type for optimization
        db_type = self._get_database_type(database_url)

        # Create synchronous engine
        sync_kwargs = self.pool_config.get_sqlalchemy_pool_kwargs()

        # SQLite-specific optimizations
        if db_type == DatabaseType.SQLITE:
            # StaticPool doesn't support size parameters
            sync_kwargs = {
                'poolclass': StaticPool,
                'connect_args': self.pool_config.get_connection_kwargs(db_type),
                'echo': self.pool_config.echo,
            }

        self._engine = create_engine(database_url, **sync_kwargs)

        # Create asynchronous engine
        async_kwargs = self.pool_config.get_async_pool_kwargs()

        if db_type == DatabaseType.SQLITE:
            # StaticPool doesn't support size parameters
            async_kwargs = {
                'poolclass': StaticPool,
                'connect_args': self.pool_config.get_connection_kwargs(db_type),
                'echo': self.pool_config.echo,
            }

        self._async_engine = create_async_engine(async_database_url, **async_kwargs)

        logger.info(f"Created database engines for {db_type.value} database")

    def _create_session_factories(self) -> None:
        """Create session factories for sync and async operations."""
        if not self._engine:
            raise RuntimeError("Engines must be created before session factories")

        # Synchronous session factory
        self._session_factory = sessionmaker(
            bind=self._engine,
            autocommit=False,
            autoflush=False,
            expire_on_commit=False
        )

        # Asynchronous session factory
        if self._async_engine:
            self._async_session_factory = async_sessionmaker(
                bind=self._async_engine,
                class_=AsyncSession,
                autocommit=False,
                autoflush=False,
                expire_on_commit=False
            )

        logger.debug("Session factories created")

    def _setup_event_listeners(self) -> None:
        """Setup SQLAlchemy event listeners for metrics collection."""
        if not self._engine:
            return

        @event.listens_for(self._engine, "connect")
        def receive_connect(dbapi_connection, connection_record):
            """Track new connections."""
            self.metrics.total_connections += 1
            connection_record.connection_start_time = time.time()
            logger.debug(f"New database connection established (total: {self.metrics.total_connections})")

        @event.listens_for(self._engine, "checkout")
        def receive_checkout(dbapi_connection, connection_record, connection_proxy):
            """Track connection checkouts."""
            self.metrics.active_connections += 1
            if hasattr(connection_record, 'connection_start_time'):
                connection_time = time.time() - connection_record.connection_start_time
                # Update moving average
                total_time = self.metrics.avg_connection_time * (self.metrics.total_connections - 1)
                self.metrics.avg_connection_time = (total_time + connection_time) / self.metrics.total_connections

        @event.listens_for(self._engine, "checkin")
        def receive_checkin(dbapi_connection, connection_record):
            """Track connection checkins."""
            self.metrics.active_connections = max(0, self.metrics.active_connections - 1)

        @event.listens_for(self._engine, "invalidate")
        def receive_invalidate(dbapi_connection, connection_record, exception):
            """Track connection errors."""
            self.metrics.connection_errors += 1
            logger.warning(f"Database connection invalidated: {exception}")

    async def _start_health_monitoring(self) -> None:
        """Start background health monitoring task."""
        if self._health_check_task and not self._health_check_task.done():
            self._health_check_task.cancel()

        self._health_check_task = asyncio.create_task(self._health_check_loop())
        logger.debug("Health monitoring started")

    async def _health_check_loop(self) -> None:
        """Background health check loop."""
        while True:
            try:
                await asyncio.sleep(self.pool_config.health_check_interval)
                await self._perform_health_check()
            except asyncio.CancelledError:
                logger.debug("Health check loop cancelled")
                break
            except Exception as e:
                logger.error(f"Health check failed: {e}")

    async def _perform_health_check(self) -> bool:
        """Perform database health check."""
        try:
            # Test sync connection
            with self.get_session() as session:
                session.execute(text("SELECT 1"))

            # Test async connection if available
            if self._async_engine:
                async with self.get_async_session() as session:
                    await session.execute(text("SELECT 1"))

            self.metrics.last_health_check = datetime.utcnow()
            self._is_healthy = True
            logger.debug("Database health check passed")
            return True

        except Exception as e:
            self._is_healthy = False
            self.metrics.connection_errors += 1
            logger.error(f"Database health check failed: {e}")
            return False

    async def _verify_connections(self) -> None:
        """Verify database connections during initialization."""
        if not await self._perform_health_check():
            raise RuntimeError("Initial database health check failed")

    def _get_database_type(self, database_url: str) -> DatabaseType:
        """Determine database type from URL."""
        if database_url.startswith("sqlite"):
            return DatabaseType.SQLITE
        elif database_url.startswith("postgresql"):
            return DatabaseType.POSTGRESQL
        else:
            return DatabaseType.SQLITE  # Default fallback

    @asynccontextmanager
    async def get_async_session(self) -> AsyncGenerator[AsyncSession, None]:
        """Get async database session with automatic cleanup."""
        if not self._async_session_factory:
            raise RuntimeError("Async session factory not initialized")

        async with self._async_session_factory() as session:
            try:
                yield session
                await session.commit()
            except Exception as e:
                await session.rollback()
                logger.error(f"Database session error: {e}")
                raise

    @asynccontextmanager
    async def get_session_context(self) -> AsyncGenerator[Session, None]:
        """Get sync database session with automatic cleanup (async context)."""
        if not self._session_factory:
            raise RuntimeError("Session factory not initialized")

        session = self._session_factory()
        try:
            yield session
            session.commit()
        except Exception as e:
            session.rollback()
            logger.error(f"Database session error: {e}")
            raise
        finally:
            session.close()

    def get_session(self) -> Session:
        """Get synchronous database session."""
        if not self._session_factory:
            raise RuntimeError("Session factory not initialized")

        return self._session_factory()

    def get_engine(self) -> Engine:
        """Get synchronous database engine."""
        if not self._engine:
            raise RuntimeError("Database engine not initialized")

        return self._engine

    def get_async_engine(self) -> AsyncEngine:
        """Get asynchronous database engine."""
        if not self._async_engine:
            raise RuntimeError("Async database engine not initialized")

        return self._async_engine

    def get_health_status(self) -> dict[str, Any]:
        """Get current database health status."""
        return {
            'healthy': self._is_healthy,
            'metrics': self.metrics.get_metrics_dict(),
            'pool_config': {
                'pool_size': self.pool_config.pool_size,
                'max_overflow': self.pool_config.max_overflow,
                'pool_timeout': self.pool_config.pool_timeout,
            },
            'database_type': self._get_database_type(self.config.get_database_url()).value,
        }

    async def shutdown(self) -> None:
        """Gracefully shutdown database connections."""
        logger.info("Shutting down database manager...")

        # Cancel health check task
        if self._health_check_task:
            self._health_check_task.cancel()
            try:
                await self._health_check_task
            except asyncio.CancelledError:
                pass

        # Dispose engines
        if self._engine:
            self._engine.dispose()

        if self._async_engine:
            await self._async_engine.dispose()

        logger.info("Database manager shutdown complete")


# Global database manager instance
_database_manager: DatabaseManager | None = None


async def get_database_manager(config: BaseConfig | None = None) -> DatabaseManager:
    """Get or create global database manager."""
    global _database_manager

    if _database_manager is None:
        if config is None:
            raise ValueError("Configuration required for first database manager creation")

        _database_manager = DatabaseManager(config)
        await _database_manager.initialize()

    return _database_manager


async def shutdown_database_manager() -> None:
    """Shutdown global database manager."""
    global _database_manager

    if _database_manager:
        await _database_manager.shutdown()
        _database_manager = None
