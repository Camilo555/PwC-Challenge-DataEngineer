"""
Unit of Work Pattern Implementation
Provides transaction management and coordinated repository access.
"""
import asyncio
from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

from core.logging import get_logger
from data_access.repositories.base_repository import IRepository, RepositoryFactory

logger = get_logger(__name__)


class UnitOfWork:
    """
    Unit of Work pattern for managing transactions and repositories.
    Provides atomic operations across multiple repositories.
    """

    def __init__(self, session: AsyncSession):
        self.session = session
        self.repositories: dict[str, IRepository] = {}
        self.committed = False
        self._factory = RepositoryFactory(session)
        self._events: list[Callable] = []

    async def __aenter__(self):
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit."""
        if exc_type:
            await self.rollback()
        elif not self.committed:
            await self.rollback()
        await self.session.close()

    async def commit(self):
        """Commit the transaction and publish events."""
        try:
            await self.session.commit()
            self.committed = True
            await self._publish_events()
            logger.info("Transaction committed successfully")
        except Exception as e:
            await self.rollback()
            logger.error(f"Failed to commit transaction: {e}")
            raise

    async def rollback(self):
        """Rollback the transaction."""
        try:
            await self.session.rollback()
            logger.info("Transaction rolled back")
        except Exception as e:
            logger.error(f"Failed to rollback transaction: {e}")
            raise

    def get_repository(self, model_class: type) -> IRepository:
        """Get repository for model class."""
        repo_name = model_class.__name__
        if repo_name not in self.repositories:
            self.repositories[repo_name] = self._factory.create_repository(model_class)
        return self.repositories[repo_name]

    def add_event(self, event_handler: Callable):
        """Add event to be published after commit."""
        self._events.append(event_handler)

    async def _publish_events(self):
        """Publish all registered events."""
        for event_handler in self._events:
            try:
                if asyncio.iscoroutinefunction(event_handler):
                    await event_handler()
                else:
                    event_handler()
            except Exception as e:
                logger.error(f"Error publishing event: {e}")

        self._events.clear()

    async def flush(self):
        """Flush pending changes without committing."""
        await self.session.flush()

    async def refresh(self, instance):
        """Refresh instance from database."""
        await self.session.refresh(instance)


class UnitOfWorkManager:
    """
    Manager for creating and managing Unit of Work instances.
    Provides session lifecycle management and dependency injection.
    """

    def __init__(self, session_maker):
        self.session_maker = session_maker

    @asynccontextmanager
    async def get_unit_of_work(self) -> AsyncGenerator[UnitOfWork, None]:
        """Get Unit of Work with automatic session management."""
        async with self.session_maker() as session:
            uow = UnitOfWork(session)
            try:
                yield uow
            except Exception:
                await uow.rollback()
                raise
            finally:
                if not uow.committed:
                    await uow.rollback()


# Transaction decorators and utilities

def transactional(uow_manager: UnitOfWorkManager):
    """Decorator for transactional operations."""
    def decorator(func):
        async def wrapper(*args, **kwargs):
            async with uow_manager.get_unit_of_work() as uow:
                result = await func(uow, *args, **kwargs)
                await uow.commit()
                return result
        return wrapper
    return decorator


class TransactionalService:
    """Base class for services that need transactional operations."""

    def __init__(self, uow_manager: UnitOfWorkManager):
        self.uow_manager = uow_manager

    @asynccontextmanager
    async def transaction(self) -> AsyncGenerator[UnitOfWork, None]:
        """Start a new transaction."""
        async with self.uow_manager.get_unit_of_work() as uow:
            yield uow
            await uow.commit()


# Batch operations with Unit of Work

class BatchProcessor:
    """Processor for batch operations with transaction management."""

    def __init__(self, uow_manager: UnitOfWorkManager, batch_size: int = 1000):
        self.uow_manager = uow_manager
        self.batch_size = batch_size

    async def process_batch(self, items: list[Any], processor_func: Callable):
        """Process items in batches with separate transactions."""
        total_processed = 0
        total_failed = 0
        errors = []

        for i in range(0, len(items), self.batch_size):
            batch = items[i:i + self.batch_size]

            try:
                async with self.uow_manager.get_unit_of_work() as uow:
                    batch_result = await processor_func(uow, batch)
                    await uow.commit()
                    total_processed += len(batch)
                    logger.info(f"Processed batch {i//self.batch_size + 1}: {len(batch)} items")

            except Exception as e:
                total_failed += len(batch)
                error_detail = {
                    'batch_start': i,
                    'batch_size': len(batch),
                    'error': str(e)
                }
                errors.append(error_detail)
                logger.error(f"Failed to process batch {i//self.batch_size + 1}: {e}")

        return {
            'total_processed': total_processed,
            'total_failed': total_failed,
            'errors': errors
        }


# Event-driven architecture support

class DomainEvent:
    """Base class for domain events."""

    def __init__(self, **kwargs):
        self.data = kwargs
        self.timestamp = datetime.utcnow()


class EventBus:
    """Simple event bus for domain events."""

    def __init__(self):
        self._handlers: dict[type, list[Callable]] = {}

    def subscribe(self, event_type: type, handler: Callable):
        """Subscribe to an event type."""
        if event_type not in self._handlers:
            self._handlers[event_type] = []
        self._handlers[event_type].append(handler)

    async def publish(self, event: DomainEvent):
        """Publish an event to all subscribers."""
        event_type = type(event)
        if event_type in self._handlers:
            for handler in self._handlers[event_type]:
                try:
                    if asyncio.iscoroutinefunction(handler):
                        await handler(event)
                    else:
                        handler(event)
                except Exception as e:
                    logger.error(f"Error in event handler: {e}")


# Domain events for sales operations

class SaleCreatedEvent(DomainEvent):
    """Event fired when a sale is created."""
    pass


class SaleUpdatedEvent(DomainEvent):
    """Event fired when a sale is updated."""
    pass


class SaleDeletedEvent(DomainEvent):
    """Event fired when a sale is deleted."""
    pass


class BatchOperationCompletedEvent(DomainEvent):
    """Event fired when a batch operation completes."""
    pass


# Service base class with UoW support

class BaseService:
    """Base service class with Unit of Work support."""

    def __init__(self, uow_manager: UnitOfWorkManager, event_bus: EventBus | None = None):
        self.uow_manager = uow_manager
        self.event_bus = event_bus or EventBus()

    async def execute_in_transaction(self, operation: Callable, *args, **kwargs):
        """Execute operation in a transaction."""
        async with self.uow_manager.get_unit_of_work() as uow:
            result = await operation(uow, *args, **kwargs)
            await uow.commit()
            return result

    async def publish_event(self, event: DomainEvent):
        """Publish domain event."""
        await self.event_bus.publish(event)


# Saga pattern support for distributed transactions

class SagaStep:
    """Single step in a saga."""

    def __init__(self, name: str, forward_action: Callable, compensating_action: Callable):
        self.name = name
        self.forward_action = forward_action
        self.compensating_action = compensating_action


class Saga:
    """Saga pattern implementation for distributed transactions."""

    def __init__(self, name: str):
        self.name = name
        self.steps: list[SagaStep] = []
        self.completed_steps: list[SagaStep] = []

    def add_step(self, step: SagaStep):
        """Add step to saga."""
        self.steps.append(step)

    async def execute(self, uow_manager: UnitOfWorkManager, context: dict[str, Any]):
        """Execute saga with compensation on failure."""
        try:
            # Execute forward actions
            for step in self.steps:
                async with uow_manager.get_unit_of_work() as uow:
                    await step.forward_action(uow, context)
                    await uow.commit()
                    self.completed_steps.append(step)
                    logger.info(f"Saga step completed: {step.name}")

            logger.info(f"Saga completed successfully: {self.name}")
            return True

        except Exception as e:
            logger.error(f"Saga failed at step {step.name}: {e}")
            await self._compensate(uow_manager, context)
            raise

    async def _compensate(self, uow_manager: UnitOfWorkManager, context: dict[str, Any]):
        """Execute compensating actions for completed steps."""
        logger.info(f"Starting compensation for saga: {self.name}")

        # Execute compensating actions in reverse order
        for step in reversed(self.completed_steps):
            try:
                async with uow_manager.get_unit_of_work() as uow:
                    await step.compensating_action(uow, context)
                    await uow.commit()
                    logger.info(f"Compensated step: {step.name}")
            except Exception as e:
                logger.error(f"Failed to compensate step {step.name}: {e}")
                # Continue with other compensations

        logger.info(f"Compensation completed for saga: {self.name}")


# Factory for creating UoW managers

def create_uow_manager(database_url: str, **engine_kwargs) -> UnitOfWorkManager:
    """Create Unit of Work manager with database connection."""
    engine = create_async_engine(database_url, **engine_kwargs)
    session_maker = sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False
    )
    return UnitOfWorkManager(session_maker)
