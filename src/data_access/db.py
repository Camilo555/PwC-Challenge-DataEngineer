from collections.abc import Iterator
from contextlib import contextmanager

from sqlmodel import Session, SQLModel, create_engine
from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine

from core.config import settings

_engine = create_engine(settings.get_database_url(async_mode=False), echo=False, future=True)
_async_engine = None


def get_engine():
    return _engine


async def get_async_engine() -> AsyncEngine:
    """Get or create async database engine."""
    global _async_engine
    if _async_engine is None:
        async_url = settings.get_database_url(async_mode=True)
        _async_engine = create_async_engine(async_url, echo=False, future=True)
    return _async_engine


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
    SQLModel.metadata.create_all(_engine)


def get_session() -> Iterator[Session]:
    """Get a database session for dependency injection."""
    with Session(_engine) as session:
        try:
            yield session
        finally:
            session.close()


@contextmanager
def session_scope() -> Iterator[Session]:
    """Provide a transactional scope around a series of operations."""
    with Session(_engine) as session:
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
