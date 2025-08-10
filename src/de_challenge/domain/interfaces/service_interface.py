"""Service interfaces for domain layer."""

from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime


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
    ) -> Tuple[bool, List[str]]:
        """Validate a single entity."""
        pass

    @abstractmethod
    async def validate_batch(
        self,
        entities: List[Any],
        strict: bool = False
    ) -> Dict[str, Any]:
        """Validate batch of entities."""
        pass

    @abstractmethod
    async def get_validation_report(self) -> Dict[str, Any]:
        """Get validation report."""
        pass


class ITransformationService(IService):
    """Transformation service interface."""

    @abstractmethod
    async def transform_entity(
        self,
        entity: Any,
        target_schema: Optional[str] = None
    ) -> Any:
        """Transform single entity."""
        pass

    @abstractmethod
    async def transform_batch(
        self,
        entities: List[Any],
        target_schema: Optional[str] = None
    ) -> List[Any]:
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
        filters: Optional[Dict[str, Any]] = None,
        limit: int = 100
    ) -> List[Any]:
        """Search entities."""
        pass

    @abstractmethod
    async def search_similar(
        self,
        entity: Any,
        limit: int = 10
    ) -> List[Any]:
        """Find similar entities."""
        pass

    @abstractmethod
    async def build_index(
        self,
        entities: List[Any]
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
