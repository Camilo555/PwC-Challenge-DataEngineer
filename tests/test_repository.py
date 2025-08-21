"""
Unit Tests for Repository Pattern
Tests data access layer functionality and repository patterns.
"""
import pytest
import asyncio
from datetime import datetime, timedelta
from uuid import uuid4, UUID
from typing import Optional
from unittest.mock import AsyncMock, Mock, patch

from sqlmodel import SQLModel, Field, create_engine, Session
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy import text
import pandas as pd

from data_access.repositories.base_repository import (
    AsyncSQLModelRepository, BaseSpecification, DateRangeSpecification,
    CustomerSpecification, RepositoryFactory, RepositoryRegistry
)
from data_access.patterns.unit_of_work import UnitOfWork, UnitOfWorkManager


# Test models for repository testing
class TestEntity(SQLModel, table=True):
    """Test entity for repository testing."""
    id: Optional[UUID] = Field(default_factory=uuid4, primary_key=True)
    name: str
    email: str
    age: int
    created_at: datetime = Field(default_factory=datetime.utcnow)


class TestSpecifications:
    """Test specification pattern functionality."""
    
    def test_base_specification(self):
        """Test base specification functionality."""
        # Create specification with single condition
        spec = BaseSpecification([TestEntity.age > 18])
        condition = spec.to_sql_condition()
        
        assert condition is not None
        
        # Test empty specification
        empty_spec = BaseSpecification([])
        empty_condition = empty_spec.to_sql_condition()
        
        assert empty_condition is True
    
    def test_specification_combination(self):
        """Test combining specifications with AND/OR."""
        spec1 = BaseSpecification([TestEntity.age > 18])
        spec2 = BaseSpecification([TestEntity.name.like('%john%')])
        
        # Test AND combination
        and_spec = spec1.and_(spec2)
        assert len(and_spec.conditions) == 2
        
        # Test OR combination
        or_spec = spec1.or_(spec2)
        assert len(or_spec.conditions) == 1  # OR creates single combined condition
    
    def test_date_range_specification(self):
        """Test date range specification."""
        # Mock model class with date field
        class MockModel:
            date_key = Mock()
        
        spec = DateRangeSpecification(MockModel, "2023-01-01", "2023-12-31")
        
        assert len(spec.conditions) == 2  # start and end date conditions
    
    def test_customer_specification(self):
        """Test customer specification."""
        # Mock model class with customer field
        class MockModel:
            customer_key = Mock()
        
        spec = CustomerSpecification(MockModel, "customer123")
        
        assert len(spec.conditions) == 1


class TestAsyncRepository:
    """Test async repository functionality."""
    
    @pytest.fixture
    async def async_session(self):
        """Create async test session."""
        # Use in-memory SQLite for testing
        engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        
        async with engine.begin() as conn:
            await conn.run_sync(SQLModel.metadata.create_all)
        
        from sqlalchemy.ext.asyncio import async_sessionmaker
        session_maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        
        async with session_maker() as session:
            yield session
        
        await engine.dispose()
    
    @pytest.fixture
    def repository(self, async_session):
        """Create test repository."""
        return AsyncSQLModelRepository(async_session, TestEntity)
    
    @pytest.mark.asyncio
    async def test_add_entity(self, repository, async_session):
        """Test adding entity to repository."""
        entity = TestEntity(
            name="John Doe",
            email="john@example.com",
            age=30
        )
        
        result = await repository.add(entity)
        await async_session.commit()
        
        assert result.id is not None
        assert result.name == "John Doe"
        assert result.email == "john@example.com"
    
    @pytest.mark.asyncio
    async def test_get_entity_by_id(self, repository, async_session):
        """Test getting entity by ID."""
        # Add test entity
        entity = TestEntity(
            name="Jane Smith",
            email="jane@example.com",
            age=25
        )
        
        added_entity = await repository.add(entity)
        await async_session.commit()
        
        # Get entity by ID
        retrieved = await repository.get(added_entity.id)
        
        assert retrieved is not None
        assert retrieved.id == added_entity.id
        assert retrieved.name == "Jane Smith"
    
    @pytest.mark.asyncio
    async def test_update_entity(self, repository, async_session):
        """Test updating entity."""
        # Add test entity
        entity = TestEntity(
            name="Bob Wilson",
            email="bob@example.com", 
            age=35
        )
        
        added_entity = await repository.add(entity)
        await async_session.commit()
        
        # Update entity
        added_entity.age = 36
        updated_entity = await repository.update(added_entity)
        await async_session.commit()
        
        assert updated_entity.age == 36
    
    @pytest.mark.asyncio
    async def test_delete_entity(self, repository, async_session):
        """Test deleting entity."""
        # Add test entity
        entity = TestEntity(
            name="Alice Johnson",
            email="alice@example.com",
            age=28
        )
        
        added_entity = await repository.add(entity)
        await async_session.commit()
        
        # Delete entity
        success = await repository.delete(added_entity.id)
        await async_session.commit()
        
        assert success is True
        
        # Verify deletion
        deleted_entity = await repository.get(added_entity.id)
        assert deleted_entity is None
    
    @pytest.mark.asyncio
    async def test_list_entities(self, repository, async_session):
        """Test listing entities with filtering."""
        # Add test entities
        entities = [
            TestEntity(name="Young Person", email="young@example.com", age=20),
            TestEntity(name="Adult Person", email="adult@example.com", age=30),
            TestEntity(name="Senior Person", email="senior@example.com", age=65)
        ]
        
        for entity in entities:
            await repository.add(entity)
        await async_session.commit()
        
        # Test list all
        all_entities = await repository.list()
        assert len(all_entities) >= 3
        
        # Test list with specification
        adult_spec = BaseSpecification([TestEntity.age >= 30])
        adults = await repository.list(specification=adult_spec)
        
        assert len(adults) == 2  # Adult and Senior
        assert all(entity.age >= 30 for entity in adults)
        
        # Test pagination
        paginated = await repository.list(skip=1, limit=1)
        assert len(paginated) == 1
    
    @pytest.mark.asyncio
    async def test_count_entities(self, repository, async_session):
        """Test counting entities."""
        # Add test entities
        for i in range(5):
            entity = TestEntity(
                name=f"Person {i}",
                email=f"person{i}@example.com",
                age=20 + i
            )
            await repository.add(entity)
        await async_session.commit()
        
        # Test count all
        total_count = await repository.count()
        assert total_count >= 5
        
        # Test count with specification
        young_spec = BaseSpecification([TestEntity.age < 23])
        young_count = await repository.count(specification=young_spec)
        assert young_count == 3  # Ages 20, 21, 22
    
    @pytest.mark.asyncio
    async def test_exists_entity(self, repository, async_session):
        """Test entity existence check."""
        # Add test entity
        entity = TestEntity(
            name="Test Person",
            email="test@example.com",
            age=25
        )
        
        added_entity = await repository.add(entity)
        await async_session.commit()
        
        # Test exists
        exists = await repository.exists(added_entity.id)
        assert exists is True
        
        # Test non-existent entity
        fake_id = uuid4()
        not_exists = await repository.exists(fake_id)
        assert not_exists is False
    
    @pytest.mark.asyncio
    async def test_get_by_field(self, repository, async_session):
        """Test getting entity by specific field."""
        # Add test entity
        entity = TestEntity(
            name="Unique Name",
            email="unique@example.com",
            age=30
        )
        
        await repository.add(entity)
        await async_session.commit()
        
        # Get by email
        found_entity = await repository.get_by_field("email", "unique@example.com")
        
        assert found_entity is not None
        assert found_entity.email == "unique@example.com"
        assert found_entity.name == "Unique Name"
    
    @pytest.mark.asyncio
    async def test_batch_operations(self, repository, async_session):
        """Test batch operations."""
        # Test batch add
        entities = [
            TestEntity(name=f"Batch Person {i}", email=f"batch{i}@example.com", age=20 + i)
            for i in range(3)
        ]
        
        added_entities = await repository.batch_add(entities)
        await async_session.commit()
        
        assert len(added_entities) == 3
        assert all(entity.id is not None for entity in added_entities)
        
        # Test batch delete
        entity_ids = [entity.id for entity in added_entities]
        deleted_count = await repository.batch_delete(entity_ids)
        await async_session.commit()
        
        assert deleted_count == 3


class TestRepositoryFactory:
    """Test repository factory functionality."""
    
    def test_async_repository_creation(self):
        """Test async repository creation."""
        mock_session = AsyncMock(spec=AsyncSession)
        factory = RepositoryFactory(mock_session)
        
        repository = factory.create_repository(TestEntity)
        
        assert isinstance(repository, AsyncSQLModelRepository)
        assert repository.session == mock_session
        assert repository.model_class == TestEntity
    
    def test_sync_repository_creation(self):
        """Test sync repository creation."""
        mock_session = Mock(spec=Session)
        factory = RepositoryFactory(mock_session)
        
        repository = factory.create_repository(TestEntity)
        
        # Should create sync repository (imported locally in factory)
        assert repository.session == mock_session
        assert repository.model_class == TestEntity


class TestRepositoryRegistry:
    """Test repository registry functionality."""
    
    def setup_method(self):
        """Setup test registry."""
        self.registry = RepositoryRegistry()
    
    def test_registry_without_factory(self):
        """Test registry behavior without factory set."""
        with pytest.raises(ValueError, match="Repository factory not set"):
            self.registry.get_repository(TestEntity)
    
    def test_registry_with_factory(self):
        """Test registry with factory."""
        mock_session = AsyncMock(spec=AsyncSession)
        factory = RepositoryFactory(mock_session)
        
        self.registry.set_factory(factory)
        
        # Get repository (should create new one)
        repo1 = self.registry.get_repository(TestEntity)
        assert repo1 is not None
        
        # Get repository again (should return cached one)
        repo2 = self.registry.get_repository(TestEntity)
        assert repo1 is repo2
    
    def test_custom_repository_registration(self):
        """Test custom repository registration."""
        mock_repository = Mock()
        
        self.registry.register_repository(TestEntity, mock_repository)
        
        # Should return custom repository
        repo = self.registry.get_repository(TestEntity)
        assert repo is mock_repository
    
    def test_registry_clear(self):
        """Test clearing registry."""
        mock_repository = Mock()
        self.registry.register_repository(TestEntity, mock_repository)
        
        self.registry.clear()
        
        assert len(self.registry._repositories) == 0


class TestUnitOfWork:
    """Test Unit of Work pattern."""
    
    @pytest.fixture
    async def async_session(self):
        """Create async test session."""
        engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        
        async with engine.begin() as conn:
            await conn.run_sync(SQLModel.metadata.create_all)
        
        from sqlalchemy.ext.asyncio import async_sessionmaker
        session_maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        
        async with session_maker() as session:
            yield session
        
        await engine.dispose()
    
    @pytest.mark.asyncio
    async def test_unit_of_work_context_manager(self, async_session):
        """Test UoW as async context manager."""
        async with UnitOfWork(async_session) as uow:
            assert uow.session == async_session
            assert uow.committed is False
            
            # Test commit
            await uow.commit()
            assert uow.committed is True
    
    @pytest.mark.asyncio
    async def test_unit_of_work_rollback(self, async_session):
        """Test UoW rollback functionality."""
        uow = UnitOfWork(async_session)
        
        # Mock session to track rollback calls
        async_session.rollback = AsyncMock()
        
        await uow.rollback()
        
        async_session.rollback.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_unit_of_work_repository_access(self, async_session):
        """Test repository access through UoW."""
        uow = UnitOfWork(async_session)
        
        # Get repository for TestEntity
        repository = uow.get_repository(TestEntity)
        
        assert repository is not None
        assert repository.session == async_session
        
        # Second call should return same repository instance
        repository2 = uow.get_repository(TestEntity)
        assert repository is repository2
    
    @pytest.mark.asyncio
    async def test_unit_of_work_events(self, async_session):
        """Test UoW event handling."""
        uow = UnitOfWork(async_session)
        
        # Mock event handler
        event_handler = AsyncMock()
        uow.add_event(event_handler)
        
        # Mock session commit to avoid actual DB operations
        async_session.commit = AsyncMock()
        
        await uow.commit()
        
        # Event handler should have been called
        event_handler.assert_called_once()
    
    @pytest.mark.asyncio
    async def test_unit_of_work_manager(self):
        """Test UoW manager functionality."""
        # Mock session maker
        mock_session = AsyncMock(spec=AsyncSession)
        mock_session_maker = AsyncMock()
        mock_session_maker.return_value.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session_maker.return_value.__aexit__ = AsyncMock(return_value=None)
        
        manager = UnitOfWorkManager(mock_session_maker)
        
        async with manager.get_unit_of_work() as uow:
            assert isinstance(uow, UnitOfWork)
            assert uow.session == mock_session


@pytest.mark.integration
class TestRepositoryIntegration:
    """Integration tests for repository pattern."""
    
    @pytest.mark.asyncio
    async def test_end_to_end_repository_operations(self):
        """Test complete repository workflow."""
        # Setup in-memory database
        engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        
        async with engine.begin() as conn:
            await conn.run_sync(SQLModel.metadata.create_all)
        
        from sqlalchemy.ext.asyncio import async_sessionmaker
        session_maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        
        # Create UoW manager and run operations
        manager = UnitOfWorkManager(session_maker)
        
        async with manager.get_unit_of_work() as uow:
            repository = uow.get_repository(TestEntity)
            
            # Add entities
            entities = [
                TestEntity(name="Alice", email="alice@example.com", age=25),
                TestEntity(name="Bob", email="bob@example.com", age=30),
                TestEntity(name="Charlie", email="charlie@example.com", age=35)
            ]
            
            added_entities = await repository.batch_add(entities)
            
            # Query entities
            all_entities = await repository.list()
            assert len(all_entities) == 3
            
            # Filter entities
            adult_spec = BaseSpecification([TestEntity.age >= 30])
            adults = await repository.list(specification=adult_spec)
            assert len(adults) == 2
            
            # Update entity
            alice = added_entities[0]
            alice.age = 26
            await repository.update(alice)
            
            # Count entities
            total_count = await repository.count()
            assert total_count == 3
            
            # Commit changes
            await uow.commit()
        
        await engine.dispose()
    
    @pytest.mark.asyncio
    async def test_transaction_rollback_on_error(self):
        """Test transaction rollback on error."""
        engine = create_async_engine("sqlite+aiosqlite:///:memory:")
        
        async with engine.begin() as conn:
            await conn.run_sync(SQLModel.metadata.create_all)
        
        from sqlalchemy.ext.asyncio import async_sessionmaker
        session_maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)
        
        manager = UnitOfWorkManager(session_maker)
        
        # First, add an entity successfully
        async with manager.get_unit_of_work() as uow1:
            repository = uow1.get_repository(TestEntity)
            entity = TestEntity(name="Test", email="test@example.com", age=25)
            await repository.add(entity)
            await uow1.commit()
        
        # Verify entity was added
        async with manager.get_unit_of_work() as uow2:
            repository = uow2.get_repository(TestEntity)
            count_before = await repository.count()
            assert count_before == 1
        
        # Now test rollback on error
        try:
            async with manager.get_unit_of_work() as uow3:
                repository = uow3.get_repository(TestEntity)
                
                # Add entity
                entity2 = TestEntity(name="Test2", email="test2@example.com", age=30)
                await repository.add(entity2)
                
                # Simulate error before commit
                raise Exception("Simulated error")
                
                await uow3.commit()  # This should not be reached
        except Exception:
            pass  # Expected
        
        # Verify rollback - count should still be 1
        async with manager.get_unit_of_work() as uow4:
            repository = uow4.get_repository(TestEntity)
            count_after = await repository.count()
            assert count_after == 1  # No change due to rollback
        
        await engine.dispose()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])