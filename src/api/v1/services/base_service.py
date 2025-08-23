"""
Base Service for API Layer
Provides abstract base class for service layer implementation.
"""
import builtins
from abc import ABC, abstractmethod
from typing import Any, Generic, TypeVar
from uuid import UUID

from pydantic import BaseModel
from sqlmodel import Session

from core.logging import get_logger

T = TypeVar('T', bound=BaseModel)
CreateT = TypeVar('CreateT', bound=BaseModel)
UpdateT = TypeVar('UpdateT', bound=BaseModel)

logger = get_logger(__name__)


class BaseService(ABC, Generic[T, CreateT, UpdateT]):
    """
    Abstract base service class implementing common CRUD operations.
    
    Provides standardized service layer patterns for all domain entities.
    """

    def __init__(self, session: Session):
        self.session = session

    @abstractmethod
    async def create(self, item: CreateT) -> T:
        """
        Create a new entity.
        
        Args:
            item: Entity creation data
            
        Returns:
            Created entity
        """
        pass

    @abstractmethod
    async def read(self, id: UUID) -> T | None:
        """
        Read an entity by ID.
        
        Args:
            id: Entity ID
            
        Returns:
            Entity if found, None otherwise
        """
        pass

    @abstractmethod
    async def update(self, id: UUID, item: UpdateT) -> T | None:
        """
        Update an entity.
        
        Args:
            id: Entity ID
            item: Entity update data
            
        Returns:
            Updated entity if found, None otherwise
        """
        pass

    @abstractmethod
    async def delete(self, id: UUID) -> bool:
        """
        Delete an entity.
        
        Args:
            id: Entity ID
            
        Returns:
            True if deleted, False if not found
        """
        pass

    @abstractmethod
    async def list(
        self,
        skip: int = 0,
        limit: int = 100,
        filters: dict[str, Any] | None = None
    ) -> list[T]:
        """
        List entities with pagination and filtering.
        
        Args:
            skip: Number of entities to skip
            limit: Maximum number of entities to return
            filters: Optional filters to apply
            
        Returns:
            List of entities
        """
        pass

    @abstractmethod
    async def count(self, filters: dict[str, Any] | None = None) -> int:
        """
        Count entities with optional filtering.
        
        Args:
            filters: Optional filters to apply
            
        Returns:
            Count of entities
        """
        pass

    # Optional batch operations
    async def batch_create(self, items: builtins.list[CreateT]) -> builtins.list[T]:
        """
        Create multiple entities in batch.
        
        Args:
            items: List of entities to create
            
        Returns:
            List of created entities
        """
        results = []
        for item in items:
            created = await self.create(item)
            results.append(created)
        return results

    async def batch_update(self, updates: builtins.list[dict[str, Any]]) -> builtins.list[T | None]:
        """
        Update multiple entities in batch.
        
        Args:
            updates: List of update dictionaries with 'id' and update fields
            
        Returns:
            List of updated entities
        """
        results = []
        for update_data in updates:
            entity_id = update_data.pop('id')
            updated = await self.update(entity_id, update_data)
            results.append(updated)
        return results

    async def batch_delete(self, ids: builtins.list[UUID]) -> builtins.list[bool]:
        """
        Delete multiple entities in batch.
        
        Args:
            ids: List of entity IDs to delete
            
        Returns:
            List of deletion results
        """
        results = []
        for entity_id in ids:
            deleted = await self.delete(entity_id)
            results.append(deleted)
        return results

    # Utility methods

    def _apply_filters(self, query, filters: dict[str, Any] | None):
        """
        Apply filters to a query.
        
        Args:
            query: SQLModel query
            filters: Dictionary of filters to apply
            
        Returns:
            Filtered query
        """
        if not filters:
            return query

        # This is a basic implementation - should be overridden in concrete services
        for field, value in filters.items():
            if hasattr(query.column_descriptions[0]['type'], field):
                query = query.filter(getattr(query.column_descriptions[0]['type'], field) == value)

        return query

    def _log_operation(self, operation: str, entity_id: UUID | None = None, details: str | None = None):
        """
        Log service operations for auditing.
        
        Args:
            operation: Operation name
            entity_id: Optional entity ID
            details: Optional additional details
        """
        log_message = f"Service operation: {operation}"
        if entity_id:
            log_message += f" (ID: {entity_id})"
        if details:
            log_message += f" - {details}"

        logger.info(log_message)

    def _validate_pagination(self, skip: int, limit: int) -> tuple[int, int]:
        """
        Validate and normalize pagination parameters.
        
        Args:
            skip: Skip parameter
            limit: Limit parameter
            
        Returns:
            Validated (skip, limit) tuple
        """
        skip = max(0, skip)  # Ensure non-negative
        limit = max(1, min(1000, limit))  # Ensure between 1 and 1000

        return skip, limit


class BusinessLogicMixin:
    """
    Mixin providing common business logic methods.
    """

    def validate_business_rules(self, entity: Any) -> None:
        """
        Validate business rules for an entity.
        
        Args:
            entity: Entity to validate
            
        Raises:
            ValueError: If business rules are violated
        """
        # This should be implemented by concrete services
        pass

    def calculate_derived_fields(self, entity: Any) -> Any:
        """
        Calculate derived fields for an entity.
        
        Args:
            entity: Entity to process
            
        Returns:
            Entity with calculated derived fields
        """
        # This should be implemented by concrete services
        return entity

    def audit_operation(self, operation: str, entity: Any, user_id: str | None = None) -> None:
        """
        Audit an operation for compliance and tracking.
        
        Args:
            operation: Operation performed
            entity: Entity affected
            user_id: Optional user ID who performed the operation
        """
        # In a real implementation, this would write to an audit log
        logger.info(f"Audit: {operation} on {type(entity).__name__} by user {user_id}")


class CachingMixin:
    """
    Mixin providing caching capabilities.
    """

    def _get_cache_key(self, operation: str, **kwargs) -> str:
        """
        Generate cache key for an operation.
        
        Args:
            operation: Operation name
            **kwargs: Additional parameters for key generation
            
        Returns:
            Cache key string
        """
        key_parts = [operation]
        for k, v in sorted(kwargs.items()):
            key_parts.append(f"{k}:{v}")
        return ":".join(key_parts)

    async def _get_from_cache(self, key: str) -> Any | None:
        """
        Get value from cache.
        
        Args:
            key: Cache key
            
        Returns:
            Cached value or None
        """
        # In a real implementation, this would interface with Redis or similar
        return None

    async def _set_cache(self, key: str, value: Any, ttl: int = 300) -> None:
        """
        Set value in cache.
        
        Args:
            key: Cache key
            value: Value to cache
            ttl: Time to live in seconds
        """
        # In a real implementation, this would interface with Redis or similar
        pass

    async def _invalidate_cache(self, pattern: str) -> None:
        """
        Invalidate cache entries matching a pattern.
        
        Args:
            pattern: Cache key pattern to invalidate
        """
        # In a real implementation, this would interface with Redis or similar
        pass


class MetricsMixin:
    """
    Mixin providing metrics collection capabilities.
    """

    def _record_operation_metric(self, operation: str, duration_ms: float, success: bool = True) -> None:
        """
        Record operation metrics.
        
        Args:
            operation: Operation name
            duration_ms: Operation duration in milliseconds
            success: Whether operation was successful
        """
        # In a real implementation, this would send metrics to monitoring system
        logger.debug(f"Metric: {operation} took {duration_ms}ms, success: {success}")

    def _increment_counter(self, metric_name: str, labels: dict[str, str] | None = None) -> None:
        """
        Increment a counter metric.
        
        Args:
            metric_name: Name of the metric
            labels: Optional labels for the metric
        """
        # In a real implementation, this would send to monitoring system
        pass

    def _record_histogram(self, metric_name: str, value: float, labels: dict[str, str] | None = None) -> None:
        """
        Record a histogram metric.
        
        Args:
            metric_name: Name of the metric
            value: Value to record
            labels: Optional labels for the metric
        """
        # In a real implementation, this would send to monitoring system
        pass


class EnhancedBaseService(BaseService[T, CreateT, UpdateT], BusinessLogicMixin, CachingMixin, MetricsMixin):
    """
    Enhanced base service with business logic, caching, and metrics capabilities.
    """

    def __init__(self, session: Session):
        super().__init__(session)
        self.enable_caching = True
        self.enable_metrics = True
        self.cache_ttl = 300  # 5 minutes default
