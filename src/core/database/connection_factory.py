"""
High-Performance Database Connection Factory

Provides optimized database connections with:
- Advanced connection pooling
- Automatic failover and retry logic
- Connection health monitoring
- Performance optimization
- Multi-database support
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Generator
from contextlib import contextmanager
from dataclasses import dataclass
from enum import Enum
from typing import Any
from urllib.parse import urlparse

from sqlalchemy import Engine, create_engine, event
from sqlalchemy.exc import DisconnectionError, SQLAlchemyError
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.pool import QueuePool, StaticPool

from core.logging import get_logger

logger = get_logger(__name__)


class DatabaseType(str, Enum):
    """Supported database types."""

    SQLITE = "sqlite"
    POSTGRESQL = "postgresql"
    MYSQL = "mysql"
    MSSQL = "mssql"


class ConnectionState(str, Enum):
    """Connection state enumeration."""

    HEALTHY = "healthy"
    DEGRADED = "degraded"
    FAILED = "failed"
    UNKNOWN = "unknown"


@dataclass
class ConnectionConfig:
    """Database connection configuration."""

    url: str
    pool_size: int = 10
    max_overflow: int = 20
    pool_timeout: int = 30
    pool_recycle: int = 3600
    pool_pre_ping: bool = True
    echo: bool = False
    connect_args: dict[str, Any] | None = None
    engine_options: dict[str, Any] | None = None

    def __post_init__(self):
        if self.connect_args is None:
            self.connect_args = {}
        if self.engine_options is None:
            self.engine_options = {}


@dataclass
class ConnectionMetrics:
    """Connection performance metrics."""

    total_connections: int = 0
    active_connections: int = 0
    pool_size: int = 0
    checked_out: int = 0
    overflow: int = 0
    invalid: int = 0
    total_checkouts: int = 0
    connection_errors: int = 0
    avg_checkout_time: float = 0.0
    last_error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "total_connections": self.total_connections,
            "active_connections": self.active_connections,
            "pool_size": self.pool_size,
            "checked_out": self.checked_out,
            "overflow": self.overflow,
            "invalid": self.invalid,
            "total_checkouts": self.total_checkouts,
            "connection_errors": self.connection_errors,
            "avg_checkout_time": self.avg_checkout_time,
            "utilization_pct": (self.checked_out / max(self.pool_size, 1)) * 100,
            "last_error": self.last_error,
        }


class ConnectionFactory:
    """
    High-performance database connection factory with advanced features.

    Features:
    - Optimized connection pooling
    - Health monitoring
    - Automatic retry logic
    - Performance metrics
    - Multi-database support
    """

    def __init__(self):
        self._engines: dict[str, Engine] = {}
        self._session_makers: dict[str, sessionmaker] = {}
        self._configs: dict[str, ConnectionConfig] = {}
        self._metrics: dict[str, ConnectionMetrics] = {}
        self._health_status: dict[str, ConnectionState] = {}
        self._setup_event_listeners()

    def _setup_event_listeners(self):
        """Setup SQLAlchemy event listeners for monitoring."""

        @event.listens_for(Engine, "connect", named=True)
        def receive_connect(dbapi_connection, connection_record, **kw):
            """Handle new connections."""
            engine_id = id(kw.get("engine", ""))
            if engine_id in self._metrics:
                self._metrics[engine_id].total_connections += 1
            logger.debug(f"New database connection established for engine {engine_id}")

        @event.listens_for(Engine, "checkout", named=True)
        def receive_checkout(dbapi_connection, connection_record, connection_proxy, **kw):
            """Handle connection checkout from pool."""
            engine_id = id(kw.get("engine", ""))
            if engine_id in self._metrics:
                self._metrics[engine_id].total_checkouts += 1
                self._metrics[engine_id].active_connections += 1
            logger.debug(f"Connection checked out from pool for engine {engine_id}")

        @event.listens_for(Engine, "checkin", named=True)
        def receive_checkin(dbapi_connection, connection_record, **kw):
            """Handle connection checkin to pool."""
            engine_id = id(kw.get("engine", ""))
            if engine_id in self._metrics:
                self._metrics[engine_id].active_connections = max(
                    0, self._metrics[engine_id].active_connections - 1
                )
            logger.debug(f"Connection returned to pool for engine {engine_id}")

    def register_database(self, name: str, config: ConnectionConfig) -> None:
        """Register a database configuration."""
        try:
            # Determine database type from URL
            db_type = self._detect_database_type(config.url)

            # Apply database-specific optimizations
            optimized_config = self._optimize_config(config, db_type)

            # Create engine with optimized settings
            engine = self._create_optimized_engine(optimized_config, db_type)

            # Create session maker
            session_maker = sessionmaker(bind=engine, expire_on_commit=False)

            # Store configuration and objects
            engine_id = id(engine)
            self._engines[name] = engine
            self._session_makers[name] = session_maker
            self._configs[name] = optimized_config
            self._metrics[engine_id] = ConnectionMetrics()
            self._health_status[name] = ConnectionState.UNKNOWN

            # Perform initial health check
            asyncio.create_task(self._check_connection_health(name))

            logger.info(f"Registered database '{name}' with {db_type.value} engine")

        except Exception as e:
            logger.error(f"Failed to register database '{name}': {e}")
            raise

    def _detect_database_type(self, url: str) -> DatabaseType:
        """Detect database type from connection URL."""
        parsed = urlparse(url)
        scheme = parsed.scheme.lower()

        if scheme.startswith("sqlite"):
            return DatabaseType.SQLITE
        elif scheme.startswith("postgresql"):
            return DatabaseType.POSTGRESQL
        elif scheme.startswith("mysql"):
            return DatabaseType.MYSQL
        elif scheme.startswith("mssql"):
            return DatabaseType.MSSQL
        else:
            logger.warning(f"Unknown database type for URL: {url}")
            return DatabaseType.SQLITE  # Default fallback

    def _optimize_config(self, config: ConnectionConfig, db_type: DatabaseType) -> ConnectionConfig:
        """Apply database-specific optimizations to configuration."""
        optimized = ConnectionConfig(
            url=config.url,
            pool_size=config.pool_size,
            max_overflow=config.max_overflow,
            pool_timeout=config.pool_timeout,
            pool_recycle=config.pool_recycle,
            pool_pre_ping=config.pool_pre_ping,
            echo=config.echo,
            connect_args=dict(config.connect_args),
            engine_options=dict(config.engine_options),
        )

        # Database-specific optimizations
        if db_type == DatabaseType.SQLITE:
            # SQLite optimizations
            optimized.connect_args.update(
                {
                    "check_same_thread": False,
                    "timeout": 30,
                    "isolation_level": None,  # Autocommit mode
                }
            )
            # Use StaticPool for SQLite to maintain connection
            optimized.engine_options["poolclass"] = StaticPool

        elif db_type == DatabaseType.POSTGRESQL:
            # PostgreSQL optimizations
            optimized.connect_args.update(
                {
                    "connect_timeout": 10,
                    "application_name": "pwc_data_platform",
                    "options": "-c default_transaction_isolation=read_committed",
                }
            )
            optimized.engine_options["pool_reset_on_return"] = "commit"

        elif db_type == DatabaseType.MYSQL:
            # MySQL optimizations
            optimized.connect_args.update(
                {"connect_timeout": 10, "autocommit": True, "charset": "utf8mb4"}
            )

        # Performance optimizations for all databases
        optimized.engine_options.update(
            {
                "pool_recycle": 3600,  # Recycle connections every hour
                "pool_pre_ping": True,  # Validate connections before use
                "max_identifier_length": 128,
            }
        )

        return optimized

    def _create_optimized_engine(self, config: ConnectionConfig, db_type: DatabaseType) -> Engine:
        """Create optimized SQLAlchemy engine."""
        engine_kwargs = {
            "url": config.url,
            "echo": config.echo,
            "pool_size": config.pool_size,
            "max_overflow": config.max_overflow,
            "pool_timeout": config.pool_timeout,
            "pool_recycle": config.pool_recycle,
            "pool_pre_ping": config.pool_pre_ping,
            "connect_args": config.connect_args,
            **config.engine_options,
        }

        # Apply connection pooling strategy
        if db_type == DatabaseType.SQLITE:
            # SQLite uses StaticPool to maintain single connection
            engine_kwargs["poolclass"] = StaticPool
        else:
            # Other databases use QueuePool for connection pooling
            engine_kwargs["poolclass"] = QueuePool

        return create_engine(**engine_kwargs)

    async def _check_connection_health(self, name: str) -> None:
        """Check connection health asynchronously."""
        if name not in self._engines:
            return

        engine = self._engines[name]

        try:
            # Test connection with simple query
            with engine.connect() as conn:
                conn.execute("SELECT 1")

            self._health_status[name] = ConnectionState.HEALTHY
            logger.debug(f"Database '{name}' health check passed")

        except Exception as e:
            self._health_status[name] = ConnectionState.FAILED
            engine_id = id(engine)
            if engine_id in self._metrics:
                self._metrics[engine_id].connection_errors += 1
                self._metrics[engine_id].last_error = str(e)

            logger.error(f"Database '{name}' health check failed: {e}")

    @contextmanager
    def get_connection(self, name: str, auto_retry: bool = True) -> Generator[Any, None, None]:
        """Get database connection with automatic retry."""
        if name not in self._engines:
            raise ValueError(f"Database '{name}' not registered")

        engine = self._engines[name]
        max_retries = 3 if auto_retry else 1
        last_error = None

        for attempt in range(max_retries):
            try:
                start_time = time.time()

                with engine.connect() as conn:
                    # Update metrics
                    engine_id = id(engine)
                    if engine_id in self._metrics:
                        checkout_time = time.time() - start_time
                        metrics = self._metrics[engine_id]
                        metrics.avg_checkout_time = (
                            metrics.avg_checkout_time * metrics.total_checkouts + checkout_time
                        ) / (metrics.total_checkouts + 1)

                    yield conn
                    return

            except (SQLAlchemyError, DisconnectionError) as e:
                last_error = e
                engine_id = id(engine)
                if engine_id in self._metrics:
                    self._metrics[engine_id].connection_errors += 1
                    self._metrics[engine_id].last_error = str(e)

                if attempt < max_retries - 1:
                    wait_time = 2**attempt  # Exponential backoff
                    logger.warning(
                        f"Connection failed (attempt {attempt + 1}/{max_retries}), retrying in {wait_time}s: {e}"
                    )
                    time.sleep(wait_time)

                    # Mark health as degraded
                    self._health_status[name] = ConnectionState.DEGRADED
                else:
                    # Final attempt failed
                    self._health_status[name] = ConnectionState.FAILED
                    logger.error(f"Connection failed after {max_retries} attempts: {e}")
                    raise

        if last_error:
            raise last_error

    @contextmanager
    def get_session(self, name: str, auto_retry: bool = True) -> Generator[Session, None, None]:
        """Get database session with automatic retry."""
        if name not in self._session_makers:
            raise ValueError(f"Database '{name}' not registered")

        session_maker = self._session_makers[name]
        max_retries = 3 if auto_retry else 1
        last_error = None

        for attempt in range(max_retries):
            session = session_maker()

            try:
                yield session
                session.commit()
                return

            except (SQLAlchemyError, DisconnectionError) as e:
                session.rollback()
                last_error = e

                # Update error metrics
                engine = self._engines[name]
                engine_id = id(engine)
                if engine_id in self._metrics:
                    self._metrics[engine_id].connection_errors += 1
                    self._metrics[engine_id].last_error = str(e)

                if attempt < max_retries - 1:
                    wait_time = 2**attempt
                    logger.warning(
                        f"Session operation failed (attempt {attempt + 1}/{max_retries}), retrying in {wait_time}s: {e}"
                    )
                    time.sleep(wait_time)
                else:
                    logger.error(f"Session operation failed after {max_retries} attempts: {e}")
                    raise
            finally:
                session.close()

        if last_error:
            raise last_error

    def get_engine(self, name: str) -> Engine:
        """Get SQLAlchemy engine by name."""
        if name not in self._engines:
            raise ValueError(f"Database '{name}' not registered")
        return self._engines[name]

    def get_metrics(self, name: str) -> ConnectionMetrics | None:
        """Get connection metrics for a database."""
        if name not in self._engines:
            return None

        engine = self._engines[name]
        engine_id = id(engine)
        metrics = self._metrics.get(engine_id)

        if metrics and hasattr(engine.pool, "size"):
            # Update pool metrics
            pool = engine.pool
            metrics.pool_size = getattr(pool, "size", lambda: 0)()
            metrics.checked_out = getattr(pool, "checkedout", lambda: 0)()
            metrics.overflow = getattr(pool, "overflow", lambda: 0)()
            metrics.invalid = getattr(pool, "invalidated", lambda: 0)()

        return metrics

    def get_all_metrics(self) -> dict[str, dict[str, Any]]:
        """Get metrics for all registered databases."""
        all_metrics = {}

        for name in self._engines:
            metrics = self.get_metrics(name)
            if metrics:
                all_metrics[name] = metrics.to_dict()
                all_metrics[name]["health_status"] = self._health_status.get(
                    name, ConnectionState.UNKNOWN
                ).value

        return all_metrics

    def get_health_status(self, name: str) -> ConnectionState:
        """Get health status for a database."""
        return self._health_status.get(name, ConnectionState.UNKNOWN)

    async def check_all_health(self) -> dict[str, ConnectionState]:
        """Check health status for all databases."""
        tasks = []

        for name in self._engines:
            task = asyncio.create_task(self._check_connection_health(name))
            tasks.append((name, task))

        # Wait for all health checks
        await asyncio.gather(*[task for _, task in tasks], return_exceptions=True)

        return dict(self._health_status)

    async def optimize_pools(self) -> dict[str, dict[str, Any]]:
        """Optimize connection pools based on usage patterns."""
        optimization_results = {}

        for name in self._engines:
            metrics = self.get_metrics(name)
            if not metrics:
                continue

            recommendations = []
            current_utilization = (metrics.checked_out / max(metrics.pool_size, 1)) * 100

            # Analyze utilization patterns
            if current_utilization > 80:
                recommendations.append("Consider increasing pool_size")
            elif current_utilization < 20:
                recommendations.append("Consider decreasing pool_size to save resources")

            if metrics.connection_errors > 10:
                recommendations.append("High error rate detected - check database connectivity")

            if metrics.avg_checkout_time > 5.0:  # > 5 seconds
                recommendations.append("Slow connection checkout - consider pool tuning")

            optimization_results[name] = {
                "current_metrics": metrics.to_dict(),
                "utilization_pct": current_utilization,
                "recommendations": recommendations,
                "health_status": self._health_status.get(name, ConnectionState.UNKNOWN).value,
            }

        return optimization_results

    def close_all(self) -> None:
        """Close all database engines."""
        for name, engine in self._engines.items():
            try:
                engine.dispose()
                logger.info(f"Closed database engine for '{name}'")
            except Exception as e:
                logger.error(f"Error closing engine for '{name}': {e}")

        self._engines.clear()
        self._session_makers.clear()
        self._configs.clear()
        self._metrics.clear()
        self._health_status.clear()

    def __del__(self):
        """Cleanup on destruction."""
        try:
            self.close_all()
        except Exception:
            pass  # Avoid errors during garbage collection


# Singleton instance for global use
_connection_factory: ConnectionFactory | None = None


def get_connection_factory() -> ConnectionFactory:
    """Get global connection factory instance."""
    global _connection_factory
    if _connection_factory is None:
        _connection_factory = ConnectionFactory()
    return _connection_factory


# Convenience functions
def register_database(name: str, url: str, **kwargs) -> None:
    """Convenience function to register a database."""
    config = ConnectionConfig(url=url, **kwargs)
    get_connection_factory().register_database(name, config)


def get_connection(name: str, auto_retry: bool = True):
    """Convenience function to get a database connection."""
    return get_connection_factory().get_connection(name, auto_retry)


def get_session(name: str, auto_retry: bool = True):
    """Convenience function to get a database session."""
    return get_connection_factory().get_session(name, auto_retry)


def get_engine(name: str) -> Engine:
    """Convenience function to get a database engine."""
    return get_connection_factory().get_engine(name)
