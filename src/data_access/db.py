import logging
import threading
from collections.abc import Iterator
from contextlib import contextmanager

from sqlalchemy import event
from sqlalchemy.engine import Engine
from sqlalchemy.pool import NullPool, QueuePool
from sqlmodel import Session, SQLModel, create_engine

from core.config import settings
from core.database.connection_factory import ConnectionConfig, get_connection_factory

logger = logging.getLogger(__name__)

# Thread-safe engine initialization
_engine: Engine | None = None
_engine_lock = threading.Lock()

# Connection factory integration
_connection_factory_initialized = False


def _create_engine_with_pooling() -> Engine:
    """Create database engine with proper connection pooling configuration."""

    # Connection pool settings based on environment
    if settings.environment.value == "production":
        pool_config = {
            "poolclass": QueuePool,
            "pool_size": 10,  # Number of connections to maintain
            "max_overflow": 20,  # Additional connections beyond pool_size
            "pool_timeout": 30,  # Timeout for getting connection from pool
            "pool_recycle": 3600,  # Recycle connections after 1 hour
            "pool_pre_ping": True,  # Validate connections before use
        }
    elif settings.environment.value == "testing":
        # Use NullPool for testing to avoid connection leaks
        pool_config = {
            "poolclass": NullPool,
        }
    else:
        # Development settings
        pool_config = {
            "poolclass": QueuePool,
            "pool_size": 5,
            "max_overflow": 10,
            "pool_timeout": 20,
            "pool_recycle": 1800,
            "pool_pre_ping": True,
        }

    # Additional engine configuration
    engine_config = {
        "echo": settings.environment.value == "development",
        "future": True,
        "connect_args": {
            "timeout": 20,  # Connection timeout
        },
    }

    # Merge pool and engine configurations
    engine_config.update(pool_config)

    try:
        engine = create_engine(settings.get_database_url(async_mode=False), **engine_config)

        # Add event listeners for connection monitoring
        _add_engine_events(engine)

        logger.info(f"Database engine created with pool_size={pool_config.get('pool_size', 'N/A')}")
        return engine

    except Exception as e:
        logger.error(f"Failed to create database engine: {e}")
        raise


def _add_engine_events(engine: Engine) -> None:
    """Add event listeners for connection pool monitoring."""

    @event.listens_for(engine, "connect")
    def receive_connect(dbapi_connection, connection_record):
        logger.debug("New database connection established")

    @event.listens_for(engine, "checkout")
    def receive_checkout(dbapi_connection, connection_record, connection_proxy):
        logger.debug("Connection checked out from pool")

    @event.listens_for(engine, "checkin")
    def receive_checkin(dbapi_connection, connection_record):
        logger.debug("Connection returned to pool")

    @event.listens_for(engine, "invalidate")
    def receive_invalidate(dbapi_connection, connection_record, exception):
        logger.warning(f"Connection invalidated: {exception}")


def get_engine() -> Engine:
    """Get the database engine with thread-safe initialization."""
    global _engine, _connection_factory_initialized

    if _engine is None:
        with _engine_lock:
            if _engine is None:  # Double-check locking
                _engine = _create_engine_with_pooling()

                # Initialize connection factory with optimized configuration
                if not _connection_factory_initialized:
                    _initialize_connection_factory()
                    _connection_factory_initialized = True

    return _engine


def _initialize_connection_factory() -> None:
    """Initialize connection factory with database configuration."""
    try:
        factory = get_connection_factory()

        # Register main database
        config = ConnectionConfig(
            url=settings.get_database_url(async_mode=False),
            pool_size=10 if settings.environment.value == "production" else 5,
            max_overflow=20 if settings.environment.value == "production" else 10,
            pool_timeout=30,
            pool_recycle=3600,
            pool_pre_ping=True,
            echo=settings.environment.value == "development",
            connect_args={
                "timeout": 20,
                "check_same_thread": False if "sqlite" in settings.database_url else None,
            },
            engine_options={"pool_reset_on_return": "commit", "max_identifier_length": 128},
        )

        factory.register_database("main", config)

        # Register read replica if configured (for future use)
        read_replica_url = settings.database_url  # Could be different in production
        if read_replica_url != settings.database_url:
            read_config = ConnectionConfig(
                url=read_replica_url,
                pool_size=5,
                max_overflow=10,
                pool_timeout=20,
                pool_recycle=3600,
                pool_pre_ping=True,
            )
            factory.register_database("read_replica", read_config)

        logger.info("Connection factory initialized successfully")

    except Exception as e:
        logger.error(f"Failed to initialize connection factory: {e}")
        # Continue with basic engine, don't fail startup


def get_pool_status() -> dict:
    """Get current connection pool status for monitoring."""
    engine = get_engine()
    pool = engine.pool

    if hasattr(pool, "size"):
        return {
            "pool_size": pool.size(),
            "checked_in": pool.checkedin(),
            "checked_out": pool.checkedout(),
            "overflow": pool.overflow(),
            "invalid": pool.invalid(),
        }
    else:
        return {"pool_type": "NullPool", "status": "no_pooling"}


def get_connection_factory_metrics() -> dict:
    """Get comprehensive connection factory metrics."""
    try:
        factory = get_connection_factory()
        metrics = factory.get_all_metrics()

        # Add engine pool status for comparison
        engine_pool_status = get_pool_status()
        metrics["engine_pool"] = engine_pool_status

        return metrics
    except Exception as e:
        logger.error(f"Failed to get connection factory metrics: {e}")
        return {"error": str(e)}


def create_all() -> None:
    """Create all SQLModel tables in the configured database."""
    from data_access.models.star_schema import (  # noqa: F401
        DimCountry,
        DimCustomer,
        DimDate,
        DimInvoice,
        DimProduct,
        FactSale,
    )

    SQLModel.metadata.create_all(get_engine())


def get_session() -> Iterator[Session]:
    """Get a database session for dependency injection."""
    engine = get_engine()
    with Session(engine) as session:
        try:
            yield session
        finally:
            session.close()


@contextmanager
def session_scope() -> Iterator[Session]:
    """Provide a transactional scope around a series of operations."""
    engine = get_engine()
    with Session(engine) as session:
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise


@contextmanager
def session_with_timeout(timeout: int = 30) -> Iterator[Session]:
    """Provide a session with connection timeout for long-running operations."""
    engine = get_engine()
    with Session(engine) as session:
        try:
            # Set statement timeout if supported by database
            if "postgresql" in engine.url.drivername:
                session.execute(f"SET statement_timeout = {timeout * 1000}")  # milliseconds
            elif "sqlite" in engine.url.drivername:
                # SQLite doesn't support statement_timeout, use connection timeout
                session.execute("PRAGMA busy_timeout = 30000")  # 30 seconds
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise


@contextmanager
def managed_connection(database_name: str = "main", auto_retry: bool = True) -> Iterator[any]:
    """Get a managed connection from connection factory."""
    try:
        factory = get_connection_factory()
        with factory.get_connection(database_name, auto_retry) as conn:
            yield conn
    except Exception as e:
        logger.error(f"Failed to get managed connection for '{database_name}': {e}")
        # Fallback to regular engine connection
        engine = get_engine()
        with engine.connect() as conn:
            yield conn


@contextmanager
def managed_session(database_name: str = "main", auto_retry: bool = True) -> Iterator[Session]:
    """Get a managed session from connection factory."""
    try:
        factory = get_connection_factory()
        with factory.get_session(database_name, auto_retry) as session:
            yield session
    except Exception as e:
        logger.error(f"Failed to get managed session for '{database_name}': {e}")
        # Fallback to regular session
        with session_scope() as session:
            yield session
