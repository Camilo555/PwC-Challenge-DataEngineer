"""Service interfaces for domain layer."""

from abc import ABC, abstractmethod
from typing import Any


class IService(ABC):
    """Base service interface."""

    @abstractmethod
    async def execute(self, **kwargs) -> Any:
        """Execute service operation."""
        pass


class IValidationService(IService):
    """Validation service interface."""

    @abstractmethod
    async def validate_entity(
        self,
        entity: Any,
        strict: bool = False
    ) -> tuple[bool, list[str]]:
        """Validate a single entity."""
        pass

    @abstractmethod
    async def validate_batch(
        self,
        entities: list[Any],
        strict: bool = False
    ) -> dict[str, Any]:
        """Validate batch of entities."""
        pass

    @abstractmethod
    async def get_validation_report(self) -> dict[str, Any]:
        """Get validation report."""
        pass


class ITransformationService(IService):
    """Transformation service interface."""

    @abstractmethod
    async def transform_entity(
        self,
        entity: Any,
        target_schema: str | None = None
    ) -> Any:
        """Transform single entity."""
        pass

    @abstractmethod
    async def transform_batch(
        self,
        entities: list[Any],
        target_schema: str | None = None
    ) -> list[Any]:
        """Transform batch of entities."""
        pass

    @abstractmethod
    async def apply_business_rules(
        self,
        entity: Any
    ) -> Any:
        """Apply business rules to entity."""
        pass


class ISearchService(IService):
    """Search service interface."""

    @abstractmethod
    async def search(
        self,
        query: str,
        filters: dict[str, Any] | None = None,
        limit: int = 100
    ) -> list[Any]:
        """Search entities."""
        pass

    @abstractmethod
    async def search_similar(
        self,
        entity: Any,
        limit: int = 10
    ) -> list[Any]:
        """Find similar entities."""
        pass

    @abstractmethod
    async def build_index(
        self,
        entities: list[Any]
    ) -> bool:
        """Build search index."""
        pass

    @abstractmethod
    async def update_index(
        self,
        entity: Any
    ) -> bool:
        """Update search index for entity."""
        pass
