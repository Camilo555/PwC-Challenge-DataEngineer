from collections.abc import Iterator
from contextlib import contextmanager

from sqlmodel import Session, SQLModel, create_engine

from de_challenge.core.config import settings

_engine = create_engine(settings.get_database_url(async_mode=False), echo=False, future=True)


def get_engine():
    return _engine


def create_all() -> None:
    """Create all SQLModel tables in the configured database."""
    from de_challenge.data_access.models.star_schema import (  # noqa: F401
        DimCountry,
        DimCustomer,
        DimDate,
        DimInvoice,
        DimProduct,
        FactSale,
    )
    SQLModel.metadata.create_all(_engine)


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
