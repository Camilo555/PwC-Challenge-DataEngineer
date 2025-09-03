#!/usr/bin/env python3
"""
Schema Evolution Package

Provides comprehensive schema evolution support across medallion architecture layers
with version management, compatibility checking, and automated migration capabilities.
"""

from .medallion_schema_handler import MedallionSchemaHandler, create_medallion_handler
from .schema_manager import (
    CompatibilityLevel,
    MedallionLayer,
    SchemaChangeType,
    SchemaEvolutionManager,
    SchemaEvolutionRule,
    SchemaVersion,
    create_schema_manager,
)

__all__ = [
    # Core schema management
    'SchemaEvolutionManager',
    'SchemaVersion',
    'SchemaChangeType',
    'CompatibilityLevel',
    'MedallionLayer',
    'SchemaEvolutionRule',
    'create_schema_manager',

    # Medallion-specific handling
    'MedallionSchemaHandler',
    'create_medallion_handler'
]
