"""
Repository Pattern Implementation
Provides data access abstractions with clean separation of concerns.
"""
from __future__ import annotations

import builtins
from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar
from uuid import UUID

from sqlalchemy import and_, func, or_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlmodel import Session, SQLModel, delete, select

# Type variables for generic repository
T = TypeVar('T', bound=SQLModel)
CreateT = TypeVar('CreateT')
UpdateT = TypeVar('UpdateT')


class ISpecification(ABC):
    """Interface for query specifications (Specification pattern)."""

    @abstractmethod
    def to_sql_condition(self) -> Any:
        """Convert specification to SQL condition."""
        pass


class BaseSpecification(ISpecification):
    """Base implementation of specification pattern."""

    def __init__(self, conditions: list[Any] | None = None):
        self.conditions = conditions or []

    def to_sql_condition(self) -> Any:
        """Convert to SQL condition using AND logic."""
        if not self.conditions:
            return True
        if len(self.conditions) == 1:
            return self.conditions[0]
        return and_(*self.conditions)

    def and_(self, other: BaseSpecification) -> BaseSpecification:
        """Combine specifications with AND."""
        combined_conditions = self.conditions + other.conditions
        return BaseSpecification(combined_conditions)

    def or_(self, other: BaseSpecification) -> BaseSpecification:
        """Combine specifications with OR."""
        return BaseSpecification([or_(self.to_sql_condition(), other.to_sql_condition())])


class IRepository(Generic[T], ABC):
    """Generic repository interface."""

    @abstractmethod
    async def add(self, entity: T) -> T:
        """Add a new entity."""
        pass

    @abstractmethod
    async def get(self, id: UUID) -> T | None:
        """Get entity by ID."""
        pass

    @abstractmethod
    async def update(self, entity: T) -> T:
        """Update an existing entity."""
        pass

    @abstractmethod
    async def delete(self, id: UUID) -> bool:
        """Delete entity by ID."""
        pass

    @abstractmethod
    async def list(self, specification: ISpecification | None = None,
                  skip: int = 0, limit: int = 100) -> list[T]:
        """List entities with optional filtering."""
        pass

    @abstractmethod
    async def count(self, specification: ISpecification | None = None) -> int:
        """Count entities with optional filtering."""
        pass

    @abstractmethod
    async def exists(self, id: UUID) -> bool:
        """Check if entity exists."""
        pass


class AsyncSQLModelRepository(IRepository[T]):
    """Async SQLModel repository implementation."""

    def __init__(self, session: AsyncSession, model_class: type[T]):
        self.session = session
        self.model_class = model_class

    async def add(self, entity: T) -> T:
        """Add entity to database."""
        self.session.add(entity)
        await self.session.flush()
        await self.session.refresh(entity)
        return entity

    async def get(self, id: UUID) -> T | None:
        """Get entity by primary key."""
        stmt = select(self.model_class).where(self.model_class.id == id)
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def update(self, entity: T) -> T:
        """Update entity."""
        # Entity should already be attached to session
        await self.session.flush()
        await self.session.refresh(entity)
        return entity

    async def delete(self, id: UUID) -> bool:
        """Delete entity by ID."""
        stmt = delete(self.model_class).where(self.model_class.id == id)
        result = await self.session.execute(stmt)
        return result.rowcount > 0

    async def list(self, specification: ISpecification | None = None,
                  skip: int = 0, limit: int = 100) -> list[T]:
        """List entities with filtering."""
        stmt = select(self.model_class)

        if specification:
            stmt = stmt.where(specification.to_sql_condition())

        stmt = stmt.offset(skip).limit(limit)
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def count(self, specification: ISpecification | None = None) -> int:
        """Count entities."""
        stmt = select(func.count(self.model_class.id))

        if specification:
            stmt = stmt.where(specification.to_sql_condition())

        result = await self.session.execute(stmt)
        return result.scalar() or 0

    async def exists(self, id: UUID) -> bool:
        """Check if entity exists."""
        stmt = select(func.count(self.model_class.id)).where(self.model_class.id == id)
        result = await self.session.execute(stmt)
        return (result.scalar() or 0) > 0

    async def get_by_field(self, field_name: str, value: Any) -> T | None:
        """Get entity by specific field."""
        field = getattr(self.model_class, field_name)
        stmt = select(self.model_class).where(field == value)
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none()

    async def list_by_field(self, field_name: str, value: Any,
                           skip: int = 0, limit: int = 100) -> builtins.list[T]:
        """List entities by specific field."""
        field = getattr(self.model_class, field_name)
        stmt = select(self.model_class).where(field == value).offset(skip).limit(limit)
        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def batch_add(self, entities: builtins.list[T]) -> builtins.list[T]:
        """Add multiple entities in batch."""
        for entity in entities:
            self.session.add(entity)

        await self.session.flush()

        # Refresh all entities
        for entity in entities:
            await self.session.refresh(entity)

        return entities

    async def batch_delete(self, ids: builtins.list[UUID]) -> int:
        """Delete multiple entities by IDs."""
        stmt = delete(self.model_class).where(self.model_class.id.in_(ids))
        result = await self.session.execute(stmt)
        return result.rowcount


class SyncSQLModelRepository(IRepository[T]):
    """Synchronous SQLModel repository implementation."""

    def __init__(self, session: Session, model_class: type[T]):
        self.session = session
        self.model_class = model_class

    async def add(self, entity: T) -> T:
        """Add entity to database."""
        import asyncio
        return await asyncio.get_event_loop().run_in_executor(
            None, self._sync_add, entity
        )

    def _sync_add(self, entity: T) -> T:
        """Synchronous add operation."""
        self.session.add(entity)
        self.session.flush()
        self.session.refresh(entity)
        return entity

    async def get(self, id: UUID) -> T | None:
        """Get entity by primary key."""
        import asyncio
        return await asyncio.get_event_loop().run_in_executor(
            None, lambda: self.session.get(self.model_class, id)
        )

    async def update(self, entity: T) -> T:
        """Update entity."""
        import asyncio
        return await asyncio.get_event_loop().run_in_executor(
            None, self._sync_update, entity
        )

    def _sync_update(self, entity: T) -> T:
        """Synchronous update operation."""
        self.session.flush()
        self.session.refresh(entity)
        return entity

    async def delete(self, id: UUID) -> bool:
        """Delete entity by ID."""
        import asyncio
        return await asyncio.get_event_loop().run_in_executor(
            None, self._sync_delete, id
        )

    def _sync_delete(self, id: UUID) -> bool:
        """Synchronous delete operation."""
        entity = self.session.get(self.model_class, id)
        if entity:
            self.session.delete(entity)
            self.session.flush()
            return True
        return False

    async def list(self, specification: ISpecification | None = None,
                  skip: int = 0, limit: int = 100) -> list[T]:
        """List entities with filtering."""
        import asyncio
        return await asyncio.get_event_loop().run_in_executor(
            None, self._sync_list, specification, skip, limit
        )

    def _sync_list(self, specification: ISpecification | None = None,
                  skip: int = 0, limit: int = 100) -> list[T]:
        """Synchronous list operation."""
        stmt = select(self.model_class)

        if specification:
            stmt = stmt.where(specification.to_sql_condition())

        stmt = stmt.offset(skip).limit(limit)
        result = self.session.execute(stmt)
        return list(result.scalars().all())

    async def count(self, specification: ISpecification | None = None) -> int:
        """Count entities."""
        import asyncio
        return await asyncio.get_event_loop().run_in_executor(
            None, self._sync_count, specification
        )

    def _sync_count(self, specification: ISpecification | None = None) -> int:
        """Synchronous count operation."""
        stmt = select(func.count(self.model_class.id))

        if specification:
            stmt = stmt.where(specification.to_sql_condition())

        result = self.session.execute(stmt)
        return result.scalar() or 0

    async def exists(self, id: UUID) -> bool:
        """Check if entity exists."""
        import asyncio
        return await asyncio.get_event_loop().run_in_executor(
            None, lambda: self.session.get(self.model_class, id) is not None
        )


class RepositoryFactory:
    """Factory for creating repository instances."""

    def __init__(self, session: AsyncSession | Session):
        self.session = session
        self._is_async = isinstance(session, AsyncSession)

    def create_repository(self, model_class: type[T]) -> IRepository[T]:
        """Create repository for given model class."""
        if self._is_async:
            return AsyncSQLModelRepository(self.session, model_class)
        else:
            return SyncSQLModelRepository(self.session, model_class)


# Specific repository interfaces and implementations

class ISalesRepository(IRepository):
    """Sales-specific repository interface."""

    @abstractmethod
    async def get_by_customer(self, customer_id: str, skip: int = 0, limit: int = 100) -> list[Any]:
        """Get sales by customer ID."""
        pass

    @abstractmethod
    async def get_by_date_range(self, start_date: str, end_date: str) -> list[Any]:
        """Get sales by date range."""
        pass

    @abstractmethod
    async def get_top_products(self, limit: int = 10) -> list[dict[str, Any]]:
        """Get top-selling products."""
        pass


class SalesRepository(AsyncSQLModelRepository):
    """Sales repository implementation."""

    def __init__(self, session: AsyncSession):
        from data_access.models.star_schema import FactSale
        super().__init__(session, FactSale)

    async def get_by_customer(self, customer_id: str, skip: int = 0, limit: int = 100) -> list[Any]:
        """Get sales by customer ID."""
        stmt = select(self.model_class).where(
            self.model_class.customer_key == customer_id
        ).offset(skip).limit(limit)

        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def get_by_date_range(self, start_date: str, end_date: str) -> list[Any]:
        """Get sales by date range."""
        start_key = int(start_date.replace('-', ''))
        end_key = int(end_date.replace('-', ''))

        stmt = select(self.model_class).where(
            and_(
                self.model_class.date_key >= start_key,
                self.model_class.date_key <= end_key
            )
        )

        result = await self.session.execute(stmt)
        return list(result.scalars().all())

    async def get_top_products(self, limit: int = 10) -> list[dict[str, Any]]:
        """Get top-selling products."""
        stmt = select(
            self.model_class.product_key,
            func.sum(self.model_class.total_amount).label('total_sales'),
            func.count(self.model_class.sale_id).label('sale_count')
        ).group_by(
            self.model_class.product_key
        ).order_by(
            func.sum(self.model_class.total_amount).desc()
        ).limit(limit)

        result = await self.session.execute(stmt)
        return [
            {
                'product_key': row.product_key,
                'total_sales': float(row.total_sales),
                'sale_count': row.sale_count
            }
            for row in result.all()
        ]


# Common specifications for querying

class DateRangeSpecification(BaseSpecification):
    """Specification for date range filtering."""

    def __init__(self, model_class: type, start_date: str, end_date: str, date_field: str = 'date_key'):
        start_key = int(start_date.replace('-', ''))
        end_key = int(end_date.replace('-', ''))
        date_field_attr = getattr(model_class, date_field)

        conditions = [
            date_field_attr >= start_key,
            date_field_attr <= end_key
        ]
        super().__init__(conditions)


class CustomerSpecification(BaseSpecification):
    """Specification for customer filtering."""

    def __init__(self, model_class: type, customer_id: str):
        customer_field = model_class.customer_key
        conditions = [customer_field == customer_id]
        super().__init__(conditions)


class ProductSpecification(BaseSpecification):
    """Specification for product filtering."""

    def __init__(self, model_class: type, product_key: int):
        product_field = model_class.product_key
        conditions = [product_field == product_key]
        super().__init__(conditions)


class CountrySpecification(BaseSpecification):
    """Specification for country filtering."""

    def __init__(self, model_class: type, country_key: int):
        country_field = model_class.country_key
        conditions = [country_field == country_key]
        super().__init__(conditions)


# Repository registry for dependency injection

class RepositoryRegistry:
    """Registry for managing repository instances."""

    def __init__(self):
        self._repositories: dict[type, IRepository] = {}
        self._factory: RepositoryFactory | None = None

    def set_factory(self, factory: RepositoryFactory):
        """Set the repository factory."""
        self._factory = factory

    def get_repository(self, model_class: type[T]) -> IRepository[T]:
        """Get repository for model class."""
        if model_class not in self._repositories:
            if not self._factory:
                raise ValueError("Repository factory not set")
            self._repositories[model_class] = self._factory.create_repository(model_class)

        return self._repositories[model_class]

    def register_repository(self, model_class: type[T], repository: IRepository[T]):
        """Register a custom repository implementation."""
        self._repositories[model_class] = repository

    def clear(self):
        """Clear all registered repositories."""
        self._repositories.clear()


# Global registry instance
repository_registry = RepositoryRegistry()
