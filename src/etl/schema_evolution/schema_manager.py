#!/usr/bin/env python3
"""
Medallion Schema Evolution Manager

Handles schema evolution across bronze, silver, and gold layers of the medallion architecture.
Provides backward compatibility, schema migration, and evolution tracking capabilities.
"""

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

try:
    import polars as pl
    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False

try:
    from pyspark.sql import DataFrame as SparkDataFrame
    from pyspark.sql.types import DataType, StructField, StructType
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False

import pandas as pd

from core.logging import get_logger

logger = get_logger(__name__)


class SchemaChangeType(Enum):
    """Types of schema changes that can occur."""
    COLUMN_ADDED = "column_added"
    COLUMN_REMOVED = "column_removed"
    COLUMN_RENAMED = "column_renamed"
    COLUMN_TYPE_CHANGED = "column_type_changed"
    COLUMN_NULLABLE_CHANGED = "column_nullable_changed"
    COLUMN_DEFAULT_CHANGED = "column_default_changed"
    TABLE_RENAMED = "table_renamed"
    CONSTRAINT_ADDED = "constraint_added"
    CONSTRAINT_REMOVED = "constraint_removed"
    INDEX_ADDED = "index_added"
    INDEX_REMOVED = "index_removed"


class CompatibilityLevel(Enum):
    """Levels of compatibility for schema changes."""
    BACKWARD_COMPATIBLE = "backward_compatible"  # Old readers can read new data
    FORWARD_COMPATIBLE = "forward_compatible"    # New readers can read old data
    FULL_COMPATIBLE = "full_compatible"          # Both backward and forward compatible
    BREAKING = "breaking"                        # Incompatible change


class MedallionLayer(Enum):
    """Medallion architecture layers."""
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


@dataclass
class SchemaVersion:
    """Represents a version of a schema."""
    version: str
    schema: dict[str, Any]
    timestamp: datetime
    description: str = ""
    compatibility_level: CompatibilityLevel = CompatibilityLevel.BACKWARD_COMPATIBLE
    changes: list[dict[str, Any]] = field(default_factory=list)
    checksum: str = ""

    def __post_init__(self):
        if not self.checksum:
            self.checksum = self._calculate_checksum()

    def _calculate_checksum(self) -> str:
        """Calculate MD5 checksum of the schema."""
        schema_str = json.dumps(self.schema, sort_keys=True, default=str)
        return hashlib.md5(schema_str.encode()).hexdigest()


@dataclass
class SchemaEvolutionRule:
    """Rules for handling schema evolution."""
    change_type: SchemaChangeType
    layer: MedallionLayer
    compatibility_level: CompatibilityLevel
    auto_migrate: bool = False
    transformation_function: str | None = None
    fallback_value: Any | None = None
    validation_rules: list[str] = field(default_factory=list)


class SchemaEvolutionManager:
    """
    Manages schema evolution across medallion layers.

    Features:
    - Schema versioning and tracking
    - Compatibility checking
    - Automatic migration
    - Schema registry integration
    - Backward/forward compatibility handling
    """

    def __init__(
        self,
        schema_registry_path: Path | None = None,
        enable_auto_migration: bool = True,
        compatibility_mode: str = "backward"
    ):
        self.schema_registry_path = schema_registry_path or Path("schemas")
        self.enable_auto_migration = enable_auto_migration
        self.compatibility_mode = compatibility_mode
        self.schema_registry: dict[str, list[SchemaVersion]] = {}
        self.evolution_rules: list[SchemaEvolutionRule] = []

        self._load_schema_registry()
        self._setup_default_rules()

    def _load_schema_registry(self) -> None:
        """Load schema registry from storage."""
        registry_file = self.schema_registry_path / "registry.json"
        if registry_file.exists():
            try:
                with open(registry_file) as f:
                    data = json.load(f)
                    for table_name, versions_data in data.items():
                        versions = []
                        for version_data in versions_data:
                            version = SchemaVersion(
                                version=version_data['version'],
                                schema=version_data['schema'],
                                timestamp=datetime.fromisoformat(version_data['timestamp']),
                                description=version_data.get('description', ''),
                                compatibility_level=CompatibilityLevel(
                                    version_data.get('compatibility_level', 'backward_compatible')
                                ),
                                changes=version_data.get('changes', []),
                                checksum=version_data.get('checksum', '')
                            )
                            versions.append(version)
                        self.schema_registry[table_name] = versions
                logger.info(f"Loaded schema registry with {len(self.schema_registry)} tables")
            except Exception as e:
                logger.error(f"Failed to load schema registry: {e}")
                self.schema_registry = {}

    def _save_schema_registry(self) -> None:
        """Save schema registry to storage."""
        try:
            self.schema_registry_path.mkdir(parents=True, exist_ok=True)
            registry_file = self.schema_registry_path / "registry.json"

            data = {}
            for table_name, versions in self.schema_registry.items():
                data[table_name] = []
                for version in versions:
                    version_data = {
                        'version': version.version,
                        'schema': version.schema,
                        'timestamp': version.timestamp.isoformat(),
                        'description': version.description,
                        'compatibility_level': version.compatibility_level.value,
                        'changes': version.changes,
                        'checksum': version.checksum
                    }
                    data[table_name].append(version_data)

            with open(registry_file, 'w') as f:
                json.dump(data, f, indent=2, default=str)

            logger.info(f"Saved schema registry to {registry_file}")
        except Exception as e:
            logger.error(f"Failed to save schema registry: {e}")

    def _setup_default_rules(self) -> None:
        """Setup default schema evolution rules."""
        # Bronze layer - more permissive
        self.evolution_rules.extend([
            SchemaEvolutionRule(
                change_type=SchemaChangeType.COLUMN_ADDED,
                layer=MedallionLayer.BRONZE,
                compatibility_level=CompatibilityLevel.BACKWARD_COMPATIBLE,
                auto_migrate=True,
                fallback_value=None
            ),
            SchemaEvolutionRule(
                change_type=SchemaChangeType.COLUMN_TYPE_CHANGED,
                layer=MedallionLayer.BRONZE,
                compatibility_level=CompatibilityLevel.BREAKING,
                auto_migrate=False
            )
        ])

        # Silver layer - balanced approach
        self.evolution_rules.extend([
            SchemaEvolutionRule(
                change_type=SchemaChangeType.COLUMN_ADDED,
                layer=MedallionLayer.SILVER,
                compatibility_level=CompatibilityLevel.BACKWARD_COMPATIBLE,
                auto_migrate=True
            ),
            SchemaEvolutionRule(
                change_type=SchemaChangeType.COLUMN_REMOVED,
                layer=MedallionLayer.SILVER,
                compatibility_level=CompatibilityLevel.BREAKING,
                auto_migrate=False
            )
        ])

        # Gold layer - strict compatibility
        self.evolution_rules.extend([
            SchemaEvolutionRule(
                change_type=SchemaChangeType.COLUMN_ADDED,
                layer=MedallionLayer.GOLD,
                compatibility_level=CompatibilityLevel.FULL_COMPATIBLE,
                auto_migrate=False
            ),
            SchemaEvolutionRule(
                change_type=SchemaChangeType.COLUMN_REMOVED,
                layer=MedallionLayer.GOLD,
                compatibility_level=CompatibilityLevel.BREAKING,
                auto_migrate=False
            )
        ])

    def register_schema(
        self,
        table_name: str,
        schema: dict[str, Any],
        version: str,
        layer: MedallionLayer,
        description: str = ""
    ) -> bool:
        """Register a new schema version."""
        try:
            # Check if this is the first version
            if table_name not in self.schema_registry:
                self.schema_registry[table_name] = []

            # Get previous version for change detection
            changes = []
            compatibility_level = CompatibilityLevel.BACKWARD_COMPATIBLE

            if self.schema_registry[table_name]:
                latest_version = self.schema_registry[table_name][-1]
                changes = self._detect_schema_changes(latest_version.schema, schema)
                compatibility_level = self._assess_compatibility(changes, layer)

            # Create new schema version
            schema_version = SchemaVersion(
                version=version,
                schema=schema,
                timestamp=datetime.utcnow(),
                description=description,
                compatibility_level=compatibility_level,
                changes=changes
            )

            # Add to registry
            self.schema_registry[table_name].append(schema_version)

            # Save registry
            self._save_schema_registry()

            logger.info(f"Registered schema version {version} for table {table_name}")
            return True

        except Exception as e:
            logger.error(f"Failed to register schema: {e}")
            return False

    def _detect_schema_changes(
        self,
        old_schema: dict[str, Any],
        new_schema: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Detect changes between two schemas."""
        changes = []

        old_columns = {col['name']: col for col in old_schema.get('columns', [])}
        new_columns = {col['name']: col for col in new_schema.get('columns', [])}

        # Detect added columns
        for col_name, col_info in new_columns.items():
            if col_name not in old_columns:
                changes.append({
                    'type': SchemaChangeType.COLUMN_ADDED.value,
                    'column': col_name,
                    'details': col_info
                })

        # Detect removed columns
        for col_name in old_columns:
            if col_name not in new_columns:
                changes.append({
                    'type': SchemaChangeType.COLUMN_REMOVED.value,
                    'column': col_name,
                    'details': old_columns[col_name]
                })

        # Detect column changes
        for col_name in set(old_columns.keys()) & set(new_columns.keys()):
            old_col = old_columns[col_name]
            new_col = new_columns[col_name]

            if old_col.get('type') != new_col.get('type'):
                changes.append({
                    'type': SchemaChangeType.COLUMN_TYPE_CHANGED.value,
                    'column': col_name,
                    'old_type': old_col.get('type'),
                    'new_type': new_col.get('type')
                })

            if old_col.get('nullable') != new_col.get('nullable'):
                changes.append({
                    'type': SchemaChangeType.COLUMN_NULLABLE_CHANGED.value,
                    'column': col_name,
                    'old_nullable': old_col.get('nullable'),
                    'new_nullable': new_col.get('nullable')
                })

        return changes

    def _assess_compatibility(
        self,
        changes: list[dict[str, Any]],
        layer: MedallionLayer
    ) -> CompatibilityLevel:
        """Assess compatibility level of schema changes."""
        if not changes:
            return CompatibilityLevel.FULL_COMPATIBLE

        has_breaking = False
        has_forward_incompatible = False

        for change in changes:
            change_type = SchemaChangeType(change['type'])

            # Find applicable rule
            rule = self._get_rule(change_type, layer)
            if rule:
                if rule.compatibility_level == CompatibilityLevel.BREAKING:
                    has_breaking = True
                elif rule.compatibility_level == CompatibilityLevel.FORWARD_COMPATIBLE:
                    has_forward_incompatible = True

        if has_breaking:
            return CompatibilityLevel.BREAKING
        elif has_forward_incompatible:
            return CompatibilityLevel.FORWARD_COMPATIBLE
        else:
            return CompatibilityLevel.BACKWARD_COMPATIBLE

    def _get_rule(
        self,
        change_type: SchemaChangeType,
        layer: MedallionLayer
    ) -> SchemaEvolutionRule | None:
        """Get evolution rule for a specific change type and layer."""
        for rule in self.evolution_rules:
            if rule.change_type == change_type and rule.layer == layer:
                return rule
        return None

    def get_schema_version(
        self,
        table_name: str,
        version: str | None = None
    ) -> SchemaVersion | None:
        """Get a specific schema version or the latest."""
        if table_name not in self.schema_registry:
            return None

        versions = self.schema_registry[table_name]
        if not versions:
            return None

        if version is None:
            return versions[-1]  # Latest version

        for v in versions:
            if v.version == version:
                return v

        return None

    def migrate_dataframe(
        self,
        df: pd.DataFrame | Any,  # pl.DataFrame or SparkDataFrame
        table_name: str,
        source_version: str,
        target_version: str,
        layer: MedallionLayer
    ) -> pd.DataFrame | Any:
        """Migrate a dataframe from one schema version to another."""
        try:
            source_schema_version = self.get_schema_version(table_name, source_version)
            target_schema_version = self.get_schema_version(table_name, target_version)

            if not source_schema_version or not target_schema_version:
                raise ValueError(f"Schema versions not found for {table_name}")

            # Get migration path
            migration_steps = self._get_migration_path(
                table_name, source_version, target_version
            )

            # Apply migration steps
            migrated_df = df
            for step in migration_steps:
                migrated_df = self._apply_migration_step(
                    migrated_df, step, layer
                )

            logger.info(f"Migrated {table_name} from {source_version} to {target_version}")
            return migrated_df

        except Exception as e:
            logger.error(f"Failed to migrate dataframe: {e}")
            raise

    def _get_migration_path(
        self,
        table_name: str,
        source_version: str,
        target_version: str
    ) -> list[dict[str, Any]]:
        """Get the migration path between two schema versions."""
        if table_name not in self.schema_registry:
            return []

        versions = self.schema_registry[table_name]

        # Find version indices
        source_idx = None
        target_idx = None

        for i, version in enumerate(versions):
            if version.version == source_version:
                source_idx = i
            if version.version == target_version:
                target_idx = i

        if source_idx is None or target_idx is None:
            return []

        # Get migration steps
        migration_steps = []
        if source_idx < target_idx:
            # Forward migration
            for i in range(source_idx + 1, target_idx + 1):
                migration_steps.extend(versions[i].changes)
        else:
            # Backward migration (reverse changes)
            for i in range(source_idx, target_idx, -1):
                # Reverse the changes
                for change in reversed(versions[i].changes):
                    reversed_change = self._reverse_change(change)
                    if reversed_change:
                        migration_steps.append(reversed_change)

        return migration_steps

    def _reverse_change(self, change: dict[str, Any]) -> dict[str, Any] | None:
        """Reverse a schema change for backward migration."""
        change_type = change['type']

        if change_type == SchemaChangeType.COLUMN_ADDED.value:
            return {
                'type': SchemaChangeType.COLUMN_REMOVED.value,
                'column': change['column'],
                'details': change['details']
            }
        elif change_type == SchemaChangeType.COLUMN_REMOVED.value:
            return {
                'type': SchemaChangeType.COLUMN_ADDED.value,
                'column': change['column'],
                'details': change['details']
            }
        elif change_type == SchemaChangeType.COLUMN_TYPE_CHANGED.value:
            return {
                'type': SchemaChangeType.COLUMN_TYPE_CHANGED.value,
                'column': change['column'],
                'old_type': change['new_type'],
                'new_type': change['old_type']
            }

        # Some changes can't be easily reversed
        return None

    def _apply_migration_step(
        self,
        df: pd.DataFrame | Any,
        step: dict[str, Any],
        layer: MedallionLayer
    ) -> pd.DataFrame | Any:
        """Apply a single migration step to a dataframe."""
        step['type']
        step.get('column')

        if isinstance(df, pd.DataFrame):
            return self._apply_pandas_migration(df, step, layer)
        elif POLARS_AVAILABLE and hasattr(df, 'lazy'):  # Polars DataFrame
            return self._apply_polars_migration(df, step, layer)
        elif SPARK_AVAILABLE and hasattr(df, 'sparkSession'):  # Spark DataFrame
            return self._apply_spark_migration(df, step, layer)
        else:
            logger.warning(f"Unsupported dataframe type: {type(df)}")
            return df

    def _apply_pandas_migration(
        self,
        df: pd.DataFrame,
        step: dict[str, Any],
        layer: MedallionLayer
    ) -> pd.DataFrame:
        """Apply migration step to pandas DataFrame."""
        change_type = step['type']
        column_name = step.get('column')

        if change_type == SchemaChangeType.COLUMN_ADDED.value:
            # Add column with default value
            default_value = step.get('details', {}).get('default')
            if default_value is None:
                # Determine default based on type
                col_type = step.get('details', {}).get('type', 'string')
                if col_type in ['integer', 'int64']:
                    default_value = 0
                elif col_type in ['float', 'float64']:
                    default_value = 0.0
                elif col_type in ['boolean', 'bool']:
                    default_value = False
                else:
                    default_value = None

            df[column_name] = default_value

        elif change_type == SchemaChangeType.COLUMN_REMOVED.value:
            # Remove column if it exists
            if column_name in df.columns:
                df = df.drop(columns=[column_name])

        elif change_type == SchemaChangeType.COLUMN_TYPE_CHANGED.value:
            # Change column type
            new_type = step.get('new_type')
            if column_name in df.columns and new_type:
                try:
                    if new_type in ['integer', 'int64']:
                        df[column_name] = pd.to_numeric(df[column_name], errors='coerce').astype('Int64')
                    elif new_type in ['float', 'float64']:
                        df[column_name] = pd.to_numeric(df[column_name], errors='coerce')
                    elif new_type in ['string', 'object']:
                        df[column_name] = df[column_name].astype(str)
                    elif new_type in ['boolean', 'bool']:
                        df[column_name] = df[column_name].astype(bool)
                    elif new_type == 'datetime64':
                        df[column_name] = pd.to_datetime(df[column_name], errors='coerce')
                except Exception as e:
                    logger.warning(f"Failed to convert column {column_name} to {new_type}: {e}")

        return df

    def _apply_polars_migration(
        self,
        df: Any,  # pl.DataFrame
        step: dict[str, Any],
        layer: MedallionLayer
    ) -> Any:
        """Apply migration step to Polars DataFrame."""
        if not POLARS_AVAILABLE:
            return df

        change_type = step['type']
        column_name = step.get('column')

        try:
            if change_type == SchemaChangeType.COLUMN_ADDED.value:
                # Add column with default value
                default_value = step.get('details', {}).get('default')
                if default_value is None:
                    col_type = step.get('details', {}).get('type', 'string')
                    if col_type in ['integer', 'int64']:
                        default_value = 0
                    elif col_type in ['float', 'float64']:
                        default_value = 0.0
                    elif col_type in ['boolean', 'bool']:
                        default_value = False
                    else:
                        default_value = None

                df = df.with_columns(pl.lit(default_value).alias(column_name))

            elif change_type == SchemaChangeType.COLUMN_REMOVED.value:
                # Remove column if it exists
                if column_name in df.columns:
                    df = df.drop(column_name)

        except Exception as e:
            logger.warning(f"Failed to apply Polars migration: {e}")

        return df

    def _apply_spark_migration(
        self,
        df: Any,  # SparkDataFrame
        step: dict[str, Any],
        layer: MedallionLayer
    ) -> Any:
        """Apply migration step to Spark DataFrame."""
        if not SPARK_AVAILABLE:
            return df

        change_type = step['type']
        column_name = step.get('column')

        try:
            if change_type == SchemaChangeType.COLUMN_ADDED.value:
                # Add column with default value
                from pyspark.sql.functions import lit
                default_value = step.get('details', {}).get('default')
                if default_value is None:
                    col_type = step.get('details', {}).get('type', 'string')
                    if col_type in ['integer', 'int']:
                        default_value = 0
                    elif col_type in ['double', 'float']:
                        default_value = 0.0
                    elif col_type == 'boolean':
                        default_value = False
                    else:
                        default_value = None

                df = df.withColumn(column_name, lit(default_value))

            elif change_type == SchemaChangeType.COLUMN_REMOVED.value:
                # Remove column if it exists
                if column_name in df.columns:
                    df = df.drop(column_name)

        except Exception as e:
            logger.warning(f"Failed to apply Spark migration: {e}")

        return df

    def validate_schema_compatibility(
        self,
        table_name: str,
        new_schema: dict[str, Any],
        layer: MedallionLayer
    ) -> dict[str, Any]:
        """Validate if a new schema is compatible with existing schemas."""
        result = {
            'compatible': True,
            'compatibility_level': CompatibilityLevel.FULL_COMPATIBLE.value,
            'changes': [],
            'warnings': [],
            'errors': []
        }

        try:
            if table_name not in self.schema_registry:
                # First schema version - always compatible
                return result

            latest_version = self.schema_registry[table_name][-1]
            changes = self._detect_schema_changes(latest_version.schema, new_schema)

            if not changes:
                return result

            result['changes'] = changes
            compatibility_level = self._assess_compatibility(changes, layer)
            result['compatibility_level'] = compatibility_level.value

            if compatibility_level == CompatibilityLevel.BREAKING:
                result['compatible'] = False
                result['errors'].append(
                    f"Breaking changes detected that are not allowed in {layer.value} layer"
                )

            # Check specific rules
            for change in changes:
                change_type = SchemaChangeType(change['type'])
                rule = self._get_rule(change_type, layer)

                if rule:
                    if not rule.auto_migrate and compatibility_level == CompatibilityLevel.BREAKING:
                        result['errors'].append(
                            f"Change {change_type.value} requires manual intervention"
                        )
                    elif rule.compatibility_level != CompatibilityLevel.FULL_COMPATIBLE:
                        result['warnings'].append(
                            f"Change {change_type.value} may affect data consumers"
                        )

        except Exception as e:
            result['compatible'] = False
            result['errors'].append(f"Validation failed: {e}")

        return result

    def get_schema_lineage(self, table_name: str) -> list[dict[str, Any]]:
        """Get the evolution history of a schema."""
        if table_name not in self.schema_registry:
            return []

        lineage = []
        for version in self.schema_registry[table_name]:
            lineage.append({
                'version': version.version,
                'timestamp': version.timestamp.isoformat(),
                'description': version.description,
                'compatibility_level': version.compatibility_level.value,
                'changes': version.changes,
                'checksum': version.checksum
            })

        return lineage

    def export_schema_registry(self, output_path: Path) -> bool:
        """Export the entire schema registry."""
        try:
            output_path.mkdir(parents=True, exist_ok=True)

            # Export registry
            registry_export = {}
            for table_name, versions in self.schema_registry.items():
                registry_export[table_name] = []
                for version in versions:
                    version_data = {
                        'version': version.version,
                        'schema': version.schema,
                        'timestamp': version.timestamp.isoformat(),
                        'description': version.description,
                        'compatibility_level': version.compatibility_level.value,
                        'changes': version.changes,
                        'checksum': version.checksum
                    }
                    registry_export[table_name].append(version_data)

            export_file = output_path / "schema_registry_export.json"
            with open(export_file, 'w') as f:
                json.dump(registry_export, f, indent=2, default=str)

            # Export evolution rules
            rules_export = []
            for rule in self.evolution_rules:
                rule_data = {
                    'change_type': rule.change_type.value,
                    'layer': rule.layer.value,
                    'compatibility_level': rule.compatibility_level.value,
                    'auto_migrate': rule.auto_migrate,
                    'transformation_function': rule.transformation_function,
                    'fallback_value': rule.fallback_value,
                    'validation_rules': rule.validation_rules
                }
                rules_export.append(rule_data)

            rules_file = output_path / "evolution_rules.json"
            with open(rules_file, 'w') as f:
                json.dump(rules_export, f, indent=2)

            logger.info(f"Exported schema registry to {output_path}")
            return True

        except Exception as e:
            logger.error(f"Failed to export schema registry: {e}")
            return False


def create_schema_manager(
    registry_path: Path | None = None,
    auto_migration: bool = True,
    compatibility_mode: str = "backward"
) -> SchemaEvolutionManager:
    """Factory function to create a schema evolution manager."""
    return SchemaEvolutionManager(
        schema_registry_path=registry_path,
        enable_auto_migration=auto_migration,
        compatibility_mode=compatibility_mode
    )
