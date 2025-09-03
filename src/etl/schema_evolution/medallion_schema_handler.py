#!/usr/bin/env python3
"""
Medallion Schema Handler

Specialized schema evolution handling for bronze, silver, and gold layers
with layer-specific rules and transformations.
"""

import hashlib
import json
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

try:
    import polars as pl
    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False

from core.logging import get_logger

from .schema_manager import (
    CompatibilityLevel,
    MedallionLayer,
    SchemaChangeType,
    SchemaEvolutionManager,
    SchemaEvolutionRule,
)

logger = get_logger(__name__)


class MedallionSchemaHandler:
    """
    Handles schema evolution specific to medallion architecture layers.

    Features:
    - Layer-specific schema evolution policies
    - Cross-layer schema propagation
    - Data quality preservation during evolution
    - Automated testing of schema changes
    """

    def __init__(self, schema_manager: SchemaEvolutionManager | None = None):
        self.schema_manager = schema_manager or SchemaEvolutionManager()
        self.layer_policies = self._setup_layer_policies()
        self._setup_medallion_rules()

    def _setup_layer_policies(self) -> dict[MedallionLayer, dict[str, Any]]:
        """Setup policies for each medallion layer."""
        return {
            MedallionLayer.BRONZE: {
                'allow_breaking_changes': True,
                'auto_migrate': True,
                'preserve_raw_data': True,
                'schema_validation': 'lenient',
                'compatibility_requirement': CompatibilityLevel.BACKWARD_COMPATIBLE,
                'max_schema_versions': 10,  # Keep more versions for raw data
                'quality_checks': ['structure', 'format'],
                'transformation_allowed': ['add_metadata', 'type_inference']
            },
            MedallionLayer.SILVER: {
                'allow_breaking_changes': False,
                'auto_migrate': True,
                'preserve_raw_data': False,
                'schema_validation': 'balanced',
                'compatibility_requirement': CompatibilityLevel.BACKWARD_COMPATIBLE,
                'max_schema_versions': 5,
                'quality_checks': ['structure', 'format', 'business_rules', 'data_quality'],
                'transformation_allowed': ['clean', 'normalize', 'enrich', 'validate']
            },
            MedallionLayer.GOLD: {
                'allow_breaking_changes': False,
                'auto_migrate': False,  # Require manual approval for gold
                'preserve_raw_data': False,
                'schema_validation': 'strict',
                'compatibility_requirement': CompatibilityLevel.FULL_COMPATIBLE,
                'max_schema_versions': 3,  # Keep fewer versions for aggregated data
                'quality_checks': ['structure', 'format', 'business_rules', 'data_quality', 'consistency'],
                'transformation_allowed': ['aggregate', 'calculate', 'summarize']
            }
        }

    def _setup_medallion_rules(self) -> None:
        """Setup medallion-specific evolution rules."""
        # Bronze layer rules - more permissive
        bronze_rules = [
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
                compatibility_level=CompatibilityLevel.FORWARD_COMPATIBLE,
                auto_migrate=True,
                transformation_function="safe_type_conversion"
            ),
            SchemaEvolutionRule(
                change_type=SchemaChangeType.COLUMN_REMOVED,
                layer=MedallionLayer.BRONZE,
                compatibility_level=CompatibilityLevel.FORWARD_COMPATIBLE,
                auto_migrate=True  # Store in metadata for potential recovery
            )
        ]

        # Silver layer rules - balanced
        silver_rules = [
            SchemaEvolutionRule(
                change_type=SchemaChangeType.COLUMN_ADDED,
                layer=MedallionLayer.SILVER,
                compatibility_level=CompatibilityLevel.BACKWARD_COMPATIBLE,
                auto_migrate=True,
                validation_rules=["business_rule_compliance"]
            ),
            SchemaEvolutionRule(
                change_type=SchemaChangeType.COLUMN_TYPE_CHANGED,
                layer=MedallionLayer.SILVER,
                compatibility_level=CompatibilityLevel.BREAKING,
                auto_migrate=False
            ),
            SchemaEvolutionRule(
                change_type=SchemaChangeType.COLUMN_REMOVED,
                layer=MedallionLayer.SILVER,
                compatibility_level=CompatibilityLevel.BREAKING,
                auto_migrate=False
            )
        ]

        # Gold layer rules - strict
        gold_rules = [
            SchemaEvolutionRule(
                change_type=SchemaChangeType.COLUMN_ADDED,
                layer=MedallionLayer.GOLD,
                compatibility_level=CompatibilityLevel.BACKWARD_COMPATIBLE,
                auto_migrate=False,  # Require approval
                validation_rules=["downstream_impact_assessment", "business_rule_compliance"]
            ),
            SchemaEvolutionRule(
                change_type=SchemaChangeType.COLUMN_TYPE_CHANGED,
                layer=MedallionLayer.GOLD,
                compatibility_level=CompatibilityLevel.BREAKING,
                auto_migrate=False
            ),
            SchemaEvolutionRule(
                change_type=SchemaChangeType.COLUMN_REMOVED,
                layer=MedallionLayer.GOLD,
                compatibility_level=CompatibilityLevel.BREAKING,
                auto_migrate=False
            )
        ]

        # Add rules to schema manager
        all_rules = bronze_rules + silver_rules + gold_rules
        self.schema_manager.evolution_rules.extend(all_rules)

    def evolve_bronze_schema(
        self,
        table_name: str,
        new_data: pd.DataFrame | Any,
        source_system: str = "unknown",
        auto_register: bool = True
    ) -> dict[str, Any]:
        """
        Handle schema evolution for bronze layer data.

        Bronze layer is most permissive - focuses on preserving raw data.
        """
        try:
            result = {
                'success': True,
                'schema_registered': False,
                'schema_evolved': False,
                'changes': [],
                'warnings': [],
                'evolved_data': new_data
            }

            # Infer schema from new data
            new_schema = self._infer_schema(new_data, layer=MedallionLayer.BRONZE)

            # Check if table exists in registry
            existing_version = self.schema_manager.get_schema_version(table_name)

            if existing_version is None:
                # First time seeing this table - register it
                version = f"v1.0.0_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
                success = self.schema_manager.register_schema(
                    table_name=table_name,
                    schema=new_schema,
                    version=version,
                    layer=MedallionLayer.BRONZE,
                    description=f"Initial schema for {table_name} from {source_system}"
                )
                result['schema_registered'] = success

            else:
                # Check for schema changes
                changes = self.schema_manager._detect_schema_changes(
                    existing_version.schema, new_schema
                )

                if changes:
                    result['changes'] = changes

                    # Assess compatibility
                    self.schema_manager._assess_compatibility(
                        changes, MedallionLayer.BRONZE
                    )

                    # Bronze layer allows most changes
                    if auto_register:
                        version = self._generate_version(existing_version.version, changes)
                        success = self.schema_manager.register_schema(
                            table_name=table_name,
                            schema=new_schema,
                            version=version,
                            layer=MedallionLayer.BRONZE,
                            description=f"Schema evolution from {source_system}"
                        )
                        result['schema_registered'] = success
                        result['schema_evolved'] = True

                    # Apply transformations if needed
                    result['evolved_data'] = self._apply_bronze_transformations(
                        new_data, changes, existing_version.schema
                    )

            # Add bronze-specific metadata
            result['evolved_data'] = self._add_bronze_metadata(
                result['evolved_data'], table_name, source_system
            )

            logger.info(f"Bronze schema evolution completed for {table_name}")
            return result

        except Exception as e:
            logger.error(f"Failed to evolve bronze schema for {table_name}: {e}")
            return {
                'success': False,
                'error': str(e),
                'evolved_data': new_data
            }

    def evolve_silver_schema(
        self,
        table_name: str,
        new_data: pd.DataFrame | Any,
        business_rules: dict[str, Any] | None = None,
        validate_quality: bool = True
    ) -> dict[str, Any]:
        """
        Handle schema evolution for silver layer data.

        Silver layer balances flexibility with data quality.
        """
        try:
            result = {
                'success': True,
                'schema_registered': False,
                'schema_evolved': False,
                'changes': [],
                'warnings': [],
                'quality_issues': [],
                'evolved_data': new_data
            }

            # Infer schema from new data
            new_schema = self._infer_schema(new_data, layer=MedallionLayer.SILVER)

            # Apply business rules to schema
            if business_rules:
                new_schema = self._apply_business_rules_to_schema(new_schema, business_rules)

            # Check existing schema
            existing_version = self.schema_manager.get_schema_version(table_name)

            if existing_version is None:
                # First silver version - validate against bronze if available
                bronze_table = table_name.replace('_silver', '_bronze')
                bronze_version = self.schema_manager.get_schema_version(bronze_table)

                if bronze_version:
                    # Ensure silver schema is compatible with bronze
                    compatibility_result = self._check_layer_compatibility(
                        bronze_version.schema, new_schema,
                        MedallionLayer.BRONZE, MedallionLayer.SILVER
                    )

                    if not compatibility_result['compatible']:
                        result['warnings'].extend(compatibility_result['warnings'])

                version = f"v1.0.0_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
                success = self.schema_manager.register_schema(
                    table_name=table_name,
                    schema=new_schema,
                    version=version,
                    layer=MedallionLayer.SILVER,
                    description=f"Initial silver schema for {table_name}"
                )
                result['schema_registered'] = success

            else:
                # Check for schema changes
                changes = self.schema_manager._detect_schema_changes(
                    existing_version.schema, new_schema
                )

                if changes:
                    result['changes'] = changes

                    # Silver layer has stricter rules
                    validation_result = self.schema_manager.validate_schema_compatibility(
                        table_name, new_schema, MedallionLayer.SILVER
                    )

                    if not validation_result['compatible']:
                        result['success'] = False
                        result['warnings'].extend(validation_result['errors'])
                        return result

                    # Register new version if compatible
                    version = self._generate_version(existing_version.version, changes)
                    success = self.schema_manager.register_schema(
                        table_name=table_name,
                        schema=new_schema,
                        version=version,
                        layer=MedallionLayer.SILVER,
                        description=f"Schema evolution with {len(changes)} changes"
                    )
                    result['schema_registered'] = success
                    result['schema_evolved'] = True

            # Apply silver-specific transformations
            result['evolved_data'] = self._apply_silver_transformations(
                new_data, result['changes'], business_rules
            )

            # Validate data quality if requested
            if validate_quality:
                quality_issues = self._validate_silver_quality(result['evolved_data'])
                result['quality_issues'] = quality_issues
                if quality_issues:
                    result['warnings'].extend([f"Quality issue: {issue}" for issue in quality_issues])

            logger.info(f"Silver schema evolution completed for {table_name}")
            return result

        except Exception as e:
            logger.error(f"Failed to evolve silver schema for {table_name}: {e}")
            return {
                'success': False,
                'error': str(e),
                'evolved_data': new_data
            }

    def evolve_gold_schema(
        self,
        table_name: str,
        new_data: pd.DataFrame | Any,
        approval_required: bool = True,
        downstream_systems: list[str] | None = None
    ) -> dict[str, Any]:
        """
        Handle schema evolution for gold layer data.

        Gold layer has strictest rules - requires approval for changes.
        """
        try:
            result = {
                'success': True,
                'schema_registered': False,
                'schema_evolved': False,
                'changes': [],
                'warnings': [],
                'approval_required': approval_required,
                'downstream_impact': [],
                'evolved_data': new_data
            }

            # Infer schema from new data
            new_schema = self._infer_schema(new_data, layer=MedallionLayer.GOLD)

            # Check existing schema
            existing_version = self.schema_manager.get_schema_version(table_name)

            if existing_version is None:
                # First gold version - validate against silver if available
                silver_table = table_name.replace('_gold', '_silver')
                silver_version = self.schema_manager.get_schema_version(silver_table)

                if silver_version:
                    # Ensure gold schema is compatible with silver
                    compatibility_result = self._check_layer_compatibility(
                        silver_version.schema, new_schema,
                        MedallionLayer.SILVER, MedallionLayer.GOLD
                    )

                    if not compatibility_result['compatible']:
                        result['success'] = False
                        result['warnings'].extend(compatibility_result['errors'])
                        return result

                # Gold layer always requires approval for new schemas
                if approval_required:
                    result['approval_required'] = True
                    result['success'] = False
                    result['warnings'].append("New gold schema requires approval")
                    return result

                version = f"v1.0.0_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"
                success = self.schema_manager.register_schema(
                    table_name=table_name,
                    schema=new_schema,
                    version=version,
                    layer=MedallionLayer.GOLD,
                    description=f"Initial gold schema for {table_name}"
                )
                result['schema_registered'] = success

            else:
                # Check for schema changes
                changes = self.schema_manager._detect_schema_changes(
                    existing_version.schema, new_schema
                )

                if changes:
                    result['changes'] = changes

                    # Gold layer requires strict compatibility
                    validation_result = self.schema_manager.validate_schema_compatibility(
                        table_name, new_schema, MedallionLayer.GOLD
                    )

                    if not validation_result['compatible']:
                        result['success'] = False
                        result['warnings'].extend(validation_result['errors'])
                        return result

                    # Assess downstream impact
                    if downstream_systems:
                        impact = self._assess_downstream_impact(
                            table_name, changes, downstream_systems
                        )
                        result['downstream_impact'] = impact

                        if impact:
                            result['warnings'].extend([
                                f"Downstream impact: {system}"
                                for system in impact
                            ])

                    # Gold changes require approval
                    if approval_required:
                        result['approval_required'] = True
                        result['success'] = False
                        result['warnings'].append("Gold schema changes require approval")
                        return result

                    # Register new version if approved
                    version = self._generate_version(existing_version.version, changes)
                    success = self.schema_manager.register_schema(
                        table_name=table_name,
                        schema=new_schema,
                        version=version,
                        layer=MedallionLayer.GOLD,
                        description=f"Approved schema evolution with {len(changes)} changes"
                    )
                    result['schema_registered'] = success
                    result['schema_evolved'] = True

            # Apply gold-specific transformations
            result['evolved_data'] = self._apply_gold_transformations(
                new_data, result['changes']
            )

            logger.info(f"Gold schema evolution completed for {table_name}")
            return result

        except Exception as e:
            logger.error(f"Failed to evolve gold schema for {table_name}: {e}")
            return {
                'success': False,
                'error': str(e),
                'evolved_data': new_data
            }

    def _infer_schema(
        self,
        data: pd.DataFrame | Any,
        layer: MedallionLayer
    ) -> dict[str, Any]:
        """Infer schema from dataframe with layer-specific considerations."""
        if isinstance(data, pd.DataFrame):
            return self._infer_pandas_schema(data, layer)
        elif POLARS_AVAILABLE and hasattr(data, 'dtypes'):
            return self._infer_polars_schema(data, layer)
        else:
            logger.warning(f"Unsupported data type for schema inference: {type(data)}")
            return {"columns": [], "metadata": {"layer": layer.value}}

    def _infer_pandas_schema(
        self,
        df: pd.DataFrame,
        layer: MedallionLayer
    ) -> dict[str, Any]:
        """Infer schema from pandas DataFrame."""
        columns = []
        for col_name in df.columns:
            col_info = {
                "name": str(col_name),
                "type": self._map_pandas_dtype(df[col_name].dtype),
                "nullable": df[col_name].isnull().any(),
            }

            # Add layer-specific metadata
            if layer == MedallionLayer.BRONZE:
                # Bronze preserves raw format info
                col_info["original_type"] = str(df[col_name].dtype)
                col_info["null_count"] = df[col_name].isnull().sum()

            elif layer == MedallionLayer.SILVER:
                # Silver adds quality metrics
                col_info["unique_count"] = df[col_name].nunique()
                col_info["null_percentage"] = df[col_name].isnull().mean()

            elif layer == MedallionLayer.GOLD:
                # Gold focuses on business meaning
                col_info["business_definition"] = self._infer_business_meaning(col_name)
                col_info["aggregation_type"] = self._infer_aggregation_type(col_name, df[col_name])

            columns.append(col_info)

        return {
            "columns": columns,
            "row_count": len(df),
            "metadata": {
                "layer": layer.value,
                "inferred_at": datetime.utcnow().isoformat(),
                "column_count": len(columns)
            }
        }

    def _map_pandas_dtype(self, dtype) -> str:
        """Map pandas dtype to standardized type."""
        dtype_str = str(dtype)
        if 'int' in dtype_str:
            return 'integer'
        elif 'float' in dtype_str:
            return 'float'
        elif 'bool' in dtype_str:
            return 'boolean'
        elif 'datetime' in dtype_str:
            return 'timestamp'
        elif 'object' in dtype_str:
            return 'string'
        else:
            return 'string'

    def _infer_business_meaning(self, column_name: str) -> str:
        """Infer business meaning from column name."""
        name_lower = column_name.lower()

        if 'id' in name_lower:
            return 'identifier'
        elif 'amount' in name_lower or 'price' in name_lower or 'cost' in name_lower:
            return 'monetary'
        elif 'date' in name_lower or 'time' in name_lower:
            return 'temporal'
        elif 'count' in name_lower or 'quantity' in name_lower:
            return 'numeric_measure'
        elif 'name' in name_lower or 'description' in name_lower:
            return 'textual_attribute'
        else:
            return 'unknown'

    def _infer_aggregation_type(self, column_name: str, series: pd.Series) -> str:
        """Infer appropriate aggregation type for gold layer."""
        name_lower = column_name.lower()

        if 'sum' in name_lower or 'total' in name_lower:
            return 'sum'
        elif 'avg' in name_lower or 'mean' in name_lower:
            return 'average'
        elif 'count' in name_lower:
            return 'count'
        elif 'max' in name_lower:
            return 'maximum'
        elif 'min' in name_lower:
            return 'minimum'
        elif series.dtype in ['int64', 'float64']:
            return 'sum'  # Default for numeric
        else:
            return 'first'  # Default for non-numeric

    def _generate_version(self, current_version: str, changes: list[dict[str, Any]]) -> str:
        """Generate new version number based on changes."""
        # Simple version increment logic
        if not current_version.startswith('v'):
            return f"v1.0.1_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"

        try:
            # Extract version parts
            version_part = current_version.split('_')[0][1:]  # Remove 'v' prefix
            major, minor, patch = map(int, version_part.split('.'))

            # Determine version bump based on changes
            has_breaking = any(
                change['type'] in [
                    SchemaChangeType.COLUMN_REMOVED.value,
                    SchemaChangeType.COLUMN_TYPE_CHANGED.value
                ]
                for change in changes
            )

            has_additive = any(
                change['type'] == SchemaChangeType.COLUMN_ADDED.value
                for change in changes
            )

            if has_breaking:
                major += 1
                minor = 0
                patch = 0
            elif has_additive:
                minor += 1
                patch = 0
            else:
                patch += 1

            return f"v{major}.{minor}.{patch}_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"

        except Exception:
            # Fallback
            return f"v1.0.1_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}"

    def _add_bronze_metadata(
        self,
        data: pd.DataFrame | Any,
        table_name: str,
        source_system: str
    ) -> pd.DataFrame | Any:
        """Add bronze layer metadata columns."""
        if isinstance(data, pd.DataFrame):
            data = data.copy()
            data['_bronze_ingestion_timestamp'] = datetime.utcnow()
            data['_bronze_source_system'] = source_system
            data['_bronze_table_name'] = table_name
            data['_bronze_record_hash'] = data.apply(
                lambda row: hashlib.md5(str(row.to_dict()).encode()).hexdigest()[:8],
                axis=1
            )

        return data

    def _apply_bronze_transformations(
        self,
        data: pd.DataFrame | Any,
        changes: list[dict[str, Any]],
        old_schema: dict[str, Any]
    ) -> pd.DataFrame | Any:
        """Apply bronze-specific transformations during schema evolution."""
        # Bronze layer mostly preserves data as-is
        # Just handle basic type inference and missing columns

        if isinstance(data, pd.DataFrame) and changes:
            data = data.copy()

            # Handle removed columns - store in metadata
            removed_columns = [
                change['column'] for change in changes
                if change['type'] == SchemaChangeType.COLUMN_REMOVED.value
            ]

            if removed_columns:
                data['_bronze_removed_columns'] = json.dumps(removed_columns)

        return data

    def _apply_business_rules_to_schema(
        self,
        schema: dict[str, Any],
        business_rules: dict[str, Any]
    ) -> dict[str, Any]:
        """Apply business rules to schema definition."""
        # Add business rule constraints to columns
        for column in schema.get('columns', []):
            col_name = column['name']
            if col_name in business_rules:
                rule = business_rules[col_name]
                if 'min_value' in rule:
                    column['min_value'] = rule['min_value']
                if 'max_value' in rule:
                    column['max_value'] = rule['max_value']
                if 'allowed_values' in rule:
                    column['allowed_values'] = rule['allowed_values']
                if 'required' in rule:
                    column['nullable'] = not rule['required']

        return schema

    def _check_layer_compatibility(
        self,
        source_schema: dict[str, Any],
        target_schema: dict[str, Any],
        source_layer: MedallionLayer,
        target_layer: MedallionLayer
    ) -> dict[str, Any]:
        """Check compatibility between schemas across layers."""
        result = {
            'compatible': True,
            'warnings': [],
            'errors': []
        }

        source_columns = {col['name']: col for col in source_schema.get('columns', [])}
        target_columns = {col['name']: col for col in target_schema.get('columns', [])}

        # Check for missing required columns
        for col_name, col_info in source_columns.items():
            if col_name not in target_columns:
                if not col_info.get('nullable', True):
                    result['errors'].append(f"Required column {col_name} missing in {target_layer.value}")
                    result['compatible'] = False
                else:
                    result['warnings'].append(f"Optional column {col_name} missing in {target_layer.value}")

        return result

    def _apply_silver_transformations(
        self,
        data: pd.DataFrame | Any,
        changes: list[dict[str, Any]],
        business_rules: dict[str, Any] | None = None
    ) -> pd.DataFrame | Any:
        """Apply silver-specific transformations during schema evolution."""
        if isinstance(data, pd.DataFrame) and changes:
            data = data.copy()

            # Apply business rules
            if business_rules:
                for col_name, rule in business_rules.items():
                    if col_name in data.columns:
                        # Apply validation rules
                        if 'min_value' in rule:
                            data[col_name] = data[col_name].clip(lower=rule['min_value'])
                        if 'max_value' in rule:
                            data[col_name] = data[col_name].clip(upper=rule['max_value'])

        return data

    def _validate_silver_quality(self, data: pd.DataFrame | Any) -> list[str]:
        """Validate data quality for silver layer."""
        issues = []

        if isinstance(data, pd.DataFrame):
            # Check for high null percentages
            for col in data.columns:
                if not col.startswith('_'):  # Skip metadata columns
                    null_pct = data[col].isnull().mean()
                    if null_pct > 0.5:
                        issues.append(f"High null percentage in {col}: {null_pct:.2%}")

            # Check for duplicate rows
            duplicates = data.duplicated().sum()
            if duplicates > 0:
                issues.append(f"Found {duplicates} duplicate rows")

        return issues

    def _apply_gold_transformations(
        self,
        data: pd.DataFrame | Any,
        changes: list[dict[str, Any]]
    ) -> pd.DataFrame | Any:
        """Apply gold-specific transformations during schema evolution."""
        if isinstance(data, pd.DataFrame):
            data = data.copy()

            # Add gold layer metadata
            data['_gold_processed_timestamp'] = datetime.utcnow()
            data['_gold_quality_score'] = 1.0  # Placeholder for quality scoring

        return data

    def _assess_downstream_impact(
        self,
        table_name: str,
        changes: list[dict[str, Any]],
        downstream_systems: list[str]
    ) -> list[str]:
        """Assess impact of schema changes on downstream systems."""
        impacted_systems = []

        # Simple impact assessment based on change types
        breaking_changes = [
            change for change in changes
            if change['type'] in [
                SchemaChangeType.COLUMN_REMOVED.value,
                SchemaChangeType.COLUMN_TYPE_CHANGED.value
            ]
        ]

        if breaking_changes:
            # All downstream systems potentially impacted by breaking changes
            impacted_systems = downstream_systems.copy()
        else:
            # Only some systems might be impacted by additive changes
            additive_changes = [
                change for change in changes
                if change['type'] == SchemaChangeType.COLUMN_ADDED.value
            ]

            if additive_changes:
                # Assume half the systems need to be updated for new columns
                impacted_systems = downstream_systems[:len(downstream_systems)//2]

        return impacted_systems

    def get_layer_schema_health(self, layer: MedallionLayer) -> dict[str, Any]:
        """Get schema health metrics for a specific layer."""
        health_metrics = {
            'layer': layer.value,
            'total_tables': 0,
            'avg_schema_versions': 0,
            'recent_changes': 0,
            'compatibility_issues': 0,
            'policy_violations': 0
        }

        layer_tables = []
        for table_name, versions in self.schema_manager.schema_registry.items():
            if any(v.compatibility_level for v in versions):  # Has layer info
                # Simple heuristic: assume table name indicates layer
                if layer.value in table_name:
                    layer_tables.append((table_name, versions))

        if layer_tables:
            health_metrics['total_tables'] = len(layer_tables)
            health_metrics['avg_schema_versions'] = sum(
                len(versions) for _, versions in layer_tables
            ) / len(layer_tables)

            # Count recent changes (last 7 days)
            recent_cutoff = datetime.utcnow().timestamp() - (7 * 24 * 60 * 60)
            for _, versions in layer_tables:
                for version in versions:
                    if version.timestamp.timestamp() > recent_cutoff:
                        health_metrics['recent_changes'] += len(version.changes)

        return health_metrics


def create_medallion_handler(
    schema_registry_path: Path | None = None
) -> MedallionSchemaHandler:
    """Factory function to create a medallion schema handler."""
    schema_manager = SchemaEvolutionManager(schema_registry_path=schema_registry_path)
    return MedallionSchemaHandler(schema_manager)
