"""Base domain model with common functionality."""
from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, ClassVar

from pydantic import BaseModel, ConfigDict, Field


class DomainEntity(BaseModel, ABC):
    """
    Base class for all domain entities.
    Provides common fields and validation logic.
    """

    model_config = ConfigDict(
        str_strip_whitespace=True,
        validate_assignment=True,
        arbitrary_types_allowed=True,
        use_enum_values=True,
        json_encoders={
            datetime: lambda v: v.isoformat(),
        }
    )

    # Metadata fields
    created_at: datetime | None = Field(
        default_factory=datetime.utcnow,
        description="Entity creation timestamp"
    )
    updated_at: datetime | None = Field(
        default=None,
        description="Entity last update timestamp"
    )

    # Validation tracking
    is_valid: bool = Field(
        default=True,
        description="Whether entity passed validation"
    )
    validation_errors: list[str] = Field(
        default_factory=list,
        description="List of validation errors"
    )

    @abstractmethod
    def validate_business_rules(self) -> bool:
        """
        Validate entity against business rules.
        Must be implemented by subclasses.
        """
        pass

    def add_validation_error(self, error: str) -> None:
        """Add a validation error to the entity."""
        self.validation_errors.append(error)
        self.is_valid = False

    def clear_validation_errors(self) -> None:
        """Clear all validation errors."""
        self.validation_errors = []
        self.is_valid = True

    def to_dict(self, exclude_none: bool = True) -> dict[str, Any]:
        """Convert entity to dictionary."""
        return self.model_dump(exclude_none=exclude_none)

    def to_json(self) -> str:
        """Convert entity to JSON string."""
        return self.model_dump_json(exclude_none=True)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DomainEntity:
        """Create entity from dictionary."""
        return cls(**data)

    class Meta:
        """Metadata for domain entity."""
        abstract: ClassVar[bool] = True
