#!/usr/bin/env python3
"""
Schema Evolution Integration Layer

Integrates schema evolution with existing ETL processors and medallion architecture.
Provides seamless schema handling during data processing.
"""

from pathlib import Path
from typing import Any

import pandas as pd

try:
    import polars as pl
    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False

from core.logging import get_logger

from .medallion_schema_handler import MedallionSchemaHandler, create_medallion_handler
from .schema_manager import MedallionLayer

logger = get_logger(__name__)


class SchemaAwareProcessor:
    """
    Base class for schema-aware data processors.

    Automatically handles schema evolution during data processing.
    """

    def __init__(
        self,
        schema_handler: MedallionSchemaHandler | None = None,
        auto_evolve: bool = True,
        layer: MedallionLayer | None = None
    ):
        self.schema_handler = schema_handler or create_medallion_handler()
        self.auto_evolve = auto_evolve
        self.layer = layer
        self._schema_cache = {}

    def process_with_schema_evolution(
        self,
        data: pd.DataFrame | Any,
        table_name: str,
        layer: MedallionLayer | None = None,
        **kwargs
    ) -> dict[str, Any]:
        """
        Process data with automatic schema evolution handling.

        Returns:
            Dict with processed data and schema evolution metadata
        """
        processing_layer = layer or self.layer
        if not processing_layer:
            raise ValueError("Layer must be specified")

        try:
            # Handle schema evolution based on layer
            if processing_layer == MedallionLayer.BRONZE:
                result = self.schema_handler.evolve_bronze_schema(
                    table_name=table_name,
                    new_data=data,
                    source_system=kwargs.get('source_system', 'unknown'),
                    auto_register=self.auto_evolve
                )
            elif processing_layer == MedallionLayer.SILVER:
                result = self.schema_handler.evolve_silver_schema(
                    table_name=table_name,
                    new_data=data,
                    business_rules=kwargs.get('business_rules'),
                    validate_quality=kwargs.get('validate_quality', True)
                )
            elif processing_layer == MedallionLayer.GOLD:
                result = self.schema_handler.evolve_gold_schema(
                    table_name=table_name,
                    new_data=data,
                    approval_required=kwargs.get('approval_required', True),
                    downstream_systems=kwargs.get('downstream_systems')
                )
            else:
                raise ValueError(f"Unsupported layer: {processing_layer}")

            # Log schema evolution results
            if result.get('schema_evolved'):
                logger.info(f"Schema evolved for {table_name} in {processing_layer.value} layer")
                for change in result.get('changes', []):
                    logger.info(f"  - {change['type']}: {change.get('column', 'N/A')}")

            if result.get('warnings'):
                for warning in result['warnings']:
                    logger.warning(f"Schema evolution warning for {table_name}: {warning}")

            return result

        except Exception as e:
            logger.error(f"Schema evolution failed for {table_name}: {e}")
            return {
                'success': False,
                'error': str(e),
                'evolved_data': data
            }


class BronzeSchemaProcessor(SchemaAwareProcessor):
    """Schema-aware processor for bronze layer."""

    def __init__(self, schema_handler: MedallionSchemaHandler | None = None):
        super().__init__(schema_handler, auto_evolve=True, layer=MedallionLayer.BRONZE)

    def ingest_with_schema_evolution(
        self,
        data: pd.DataFrame | Any,
        table_name: str,
        source_system: str,
        preserve_raw: bool = True
    ) -> dict[str, Any]:
        """
        Ingest data into bronze layer with schema evolution.

        Bronze layer is most permissive - focuses on data preservation.
        """
        return self.process_with_schema_evolution(
            data=data,
            table_name=table_name,
            source_system=source_system,
            preserve_raw=preserve_raw
        )

    def handle_schema_drift(
        self,
        data: pd.DataFrame | Any,
        table_name: str,
        drift_threshold: float = 0.1
    ) -> dict[str, Any]:
        """
        Handle schema drift detection and mitigation.

        Args:
            drift_threshold: Percentage of schema changes that triggers alert
        """
        try:
            existing_version = self.schema_handler.schema_manager.get_schema_version(table_name)
            if not existing_version:
                # No existing schema - no drift possible
                return {'drift_detected': False, 'drift_score': 0.0}

            # Infer current schema
            current_schema = self.schema_handler._infer_schema(data, MedallionLayer.BRONZE)

            # Compare schemas
            changes = self.schema_handler.schema_manager._detect_schema_changes(
                existing_version.schema, current_schema
            )

            # Calculate drift score
            total_columns = len(existing_version.schema.get('columns', []))
            drift_score = len(changes) / max(total_columns, 1)

            drift_detected = drift_score > drift_threshold

            if drift_detected:
                logger.warning(f"Schema drift detected for {table_name}: {drift_score:.2%}")
                for change in changes:
                    logger.warning(f"  - {change['type']}: {change.get('column', 'N/A')}")

            return {
                'drift_detected': drift_detected,
                'drift_score': drift_score,
                'changes': changes,
                'recommended_action': 'evolve_schema' if drift_detected else 'no_action'
            }

        except Exception as e:
            logger.error(f"Schema drift detection failed for {table_name}: {e}")
            return {'drift_detected': False, 'error': str(e)}


class SilverSchemaProcessor(SchemaAwareProcessor):
    """Schema-aware processor for silver layer."""

    def __init__(self, schema_handler: MedallionSchemaHandler | None = None):
        super().__init__(schema_handler, auto_evolve=True, layer=MedallionLayer.SILVER)

    def transform_with_schema_validation(
        self,
        data: pd.DataFrame | Any,
        table_name: str,
        business_rules: dict[str, Any] | None = None,
        quality_checks: list[str] | None = None
    ) -> dict[str, Any]:
        """
        Transform data for silver layer with schema validation.

        Silver layer balances flexibility with data quality.
        """
        return self.process_with_schema_evolution(
            data=data,
            table_name=table_name,
            business_rules=business_rules,
            validate_quality=True,
            quality_checks=quality_checks
        )

    def validate_business_rules(
        self,
        data: pd.DataFrame | Any,
        business_rules: dict[str, Any]
    ) -> dict[str, Any]:
        """
        Validate data against business rules.
        """
        validation_results = {
            'valid': True,
            'violations': [],
            'warnings': []
        }

        if isinstance(data, pd.DataFrame):
            for column, rules in business_rules.items():
                if column not in data.columns:
                    if rules.get('required', False):
                        validation_results['violations'].append(
                            f"Required column '{column}' is missing"
                        )
                        validation_results['valid'] = False
                    continue

                col_data = data[column]

                # Check min/max values
                if 'min_value' in rules:
                    violations = (col_data < rules['min_value']).sum()
                    if violations > 0:
                        validation_results['violations'].append(
                            f"Column '{column}' has {violations} values below minimum {rules['min_value']}"
                        )
                        validation_results['valid'] = False

                if 'max_value' in rules:
                    violations = (col_data > rules['max_value']).sum()
                    if violations > 0:
                        validation_results['violations'].append(
                            f"Column '{column}' has {violations} values above maximum {rules['max_value']}"
                        )
                        validation_results['valid'] = False

                # Check allowed values
                if 'allowed_values' in rules:
                    invalid_values = ~col_data.isin(rules['allowed_values'])
                    violations = invalid_values.sum()
                    if violations > 0:
                        validation_results['violations'].append(
                            f"Column '{column}' has {violations} invalid values"
                        )
                        validation_results['valid'] = False

                # Check null requirements
                if rules.get('required', False):
                    null_count = col_data.isnull().sum()
                    if null_count > 0:
                        validation_results['violations'].append(
                            f"Required column '{column}' has {null_count} null values"
                        )
                        validation_results['valid'] = False

        return validation_results


class GoldSchemaProcessor(SchemaAwareProcessor):
    """Schema-aware processor for gold layer."""

    def __init__(self, schema_handler: MedallionSchemaHandler | None = None):
        super().__init__(schema_handler, auto_evolve=False, layer=MedallionLayer.GOLD)

    def aggregate_with_schema_control(
        self,
        data: pd.DataFrame | Any,
        table_name: str,
        downstream_systems: list[str] | None = None,
        require_approval: bool = True
    ) -> dict[str, Any]:
        """
        Aggregate data for gold layer with strict schema control.

        Gold layer has strictest rules - requires approval for changes.
        """
        return self.process_with_schema_evolution(
            data=data,
            table_name=table_name,
            downstream_systems=downstream_systems,
            approval_required=require_approval
        )

    def assess_downstream_impact(
        self,
        table_name: str,
        proposed_changes: list[dict[str, Any]],
        downstream_systems: list[str]
    ) -> dict[str, Any]:
        """
        Assess impact of proposed schema changes on downstream systems.
        """
        impact_assessment = {
            'total_systems': len(downstream_systems),
            'impacted_systems': [],
            'impact_level': 'low',
            'recommended_actions': []
        }

        # Analyze change types
        breaking_changes = [
            change for change in proposed_changes
            if change['type'] in ['column_removed', 'column_type_changed']
        ]

        additive_changes = [
            change for change in proposed_changes
            if change['type'] == 'column_added'
        ]

        if breaking_changes:
            # All systems potentially impacted by breaking changes
            impact_assessment['impacted_systems'] = downstream_systems.copy()
            impact_assessment['impact_level'] = 'high'
            impact_assessment['recommended_actions'].extend([
                'Coordinate with downstream system owners',
                'Plan phased rollout with backward compatibility',
                'Update API documentation and contracts',
                'Consider deprecated column approach'
            ])
        elif additive_changes:
            # Some systems might need updates for new columns
            # Simulate impact based on system types
            potentially_impacted = []
            for system in downstream_systems:
                if 'dashboard' in system.lower() or 'report' in system.lower():
                    potentially_impacted.append(system)

            impact_assessment['impacted_systems'] = potentially_impacted
            impact_assessment['impact_level'] = 'medium'
            impact_assessment['recommended_actions'].extend([
                'Notify dashboard and reporting teams',
                'Update data dictionaries',
                'Consider automatic null handling'
            ])

        return impact_assessment


class SchemaEvolutionOrchestrator:
    """
    Orchestrates schema evolution across all medallion layers.

    Provides centralized coordination of schema changes.
    """

    def __init__(self, schema_handler: MedallionSchemaHandler | None = None):
        self.schema_handler = schema_handler or create_medallion_handler()
        self.bronze_processor = BronzeSchemaProcessor(schema_handler)
        self.silver_processor = SilverSchemaProcessor(schema_handler)
        self.gold_processor = GoldSchemaProcessor(schema_handler)

    def process_full_medallion_pipeline(
        self,
        raw_data: pd.DataFrame | Any,
        table_base_name: str,
        source_system: str,
        business_rules: dict[str, Any] | None = None,
        downstream_systems: list[str] | None = None
    ) -> dict[str, Any]:
        """
        Process data through full medallion pipeline with schema evolution.

        Returns comprehensive results from all layers.
        """
        pipeline_results = {
            'bronze': {},
            'silver': {},
            'gold': {},
            'overall_success': True,
            'schema_changes': []
        }

        try:
            # Bronze layer processing
            logger.info(f"Processing bronze layer for {table_base_name}")
            bronze_result = self.bronze_processor.ingest_with_schema_evolution(
                data=raw_data,
                table_name=f"{table_base_name}_bronze",
                source_system=source_system
            )
            pipeline_results['bronze'] = bronze_result

            if not bronze_result.get('success', True):
                pipeline_results['overall_success'] = False
                return pipeline_results

            # Silver layer processing
            logger.info(f"Processing silver layer for {table_base_name}")
            silver_result = self.silver_processor.transform_with_schema_validation(
                data=bronze_result['evolved_data'],
                table_name=f"{table_base_name}_silver",
                business_rules=business_rules
            )
            pipeline_results['silver'] = silver_result

            if not silver_result.get('success', True):
                pipeline_results['overall_success'] = False
                # Silver failures don't block gold processing in this example

            # Gold layer processing (using silver data if available, else bronze)
            processing_data = (
                silver_result['evolved_data']
                if silver_result.get('success', True)
                else bronze_result['evolved_data']
            )

            # For demo - create simple aggregation
            if isinstance(processing_data, pd.DataFrame):
                # Simple aggregation for gold layer
                gold_data = self._create_sample_aggregation(processing_data)
            else:
                gold_data = processing_data

            logger.info(f"Processing gold layer for {table_base_name}")
            gold_result = self.gold_processor.aggregate_with_schema_control(
                data=gold_data,
                table_name=f"{table_base_name}_gold",
                downstream_systems=downstream_systems,
                require_approval=False  # For demo purposes
            )
            pipeline_results['gold'] = gold_result

            if not gold_result.get('success', True):
                pipeline_results['overall_success'] = False

            # Collect all schema changes
            for layer_name, layer_result in pipeline_results.items():
                if isinstance(layer_result, dict) and layer_result.get('changes'):
                    pipeline_results['schema_changes'].extend([
                        {**change, 'layer': layer_name}
                        for change in layer_result['changes']
                    ])

            logger.info(f"Medallion pipeline completed for {table_base_name}")
            return pipeline_results

        except Exception as e:
            logger.error(f"Medallion pipeline failed for {table_base_name}: {e}")
            pipeline_results['overall_success'] = False
            pipeline_results['error'] = str(e)
            return pipeline_results

    def _create_sample_aggregation(self, df: pd.DataFrame) -> pd.DataFrame:
        """Create a sample aggregation for gold layer demo."""
        if df.empty:
            return df

        # Simple aggregation - just count records and basic stats
        agg_data = pd.DataFrame({
            'record_count': [len(df)],
            'processing_timestamp': [pd.Timestamp.now()],
            'column_count': [len(df.columns)]
        })

        # Add numeric aggregations if numeric columns exist
        numeric_columns = df.select_dtypes(include=['number']).columns
        if len(numeric_columns) > 0:
            for col in numeric_columns[:3]:  # Limit to first 3 numeric columns
                agg_data[f'{col}_sum'] = [df[col].sum()]
                agg_data[f'{col}_avg'] = [df[col].mean()]

        return agg_data

    def get_pipeline_schema_health(self, table_base_name: str) -> dict[str, Any]:
        """Get schema health across entire medallion pipeline."""
        health_report = {
            'table_base_name': table_base_name,
            'layers': {},
            'cross_layer_compatibility': {},
            'recommendations': []
        }

        # Check each layer
        for layer in [MedallionLayer.BRONZE, MedallionLayer.SILVER, MedallionLayer.GOLD]:
            layer_health = self.schema_handler.get_layer_schema_health(layer)
            health_report['layers'][layer.value] = layer_health

        # Check cross-layer compatibility
        bronze_schema = self.schema_handler.schema_manager.get_schema_version(
            f"{table_base_name}_bronze"
        )
        silver_schema = self.schema_handler.schema_manager.get_schema_version(
            f"{table_base_name}_silver"
        )
        gold_schema = self.schema_handler.schema_manager.get_schema_version(
            f"{table_base_name}_gold"
        )

        if bronze_schema and silver_schema:
            compatibility = self.schema_handler._check_layer_compatibility(
                bronze_schema.schema, silver_schema.schema,
                MedallionLayer.BRONZE, MedallionLayer.SILVER
            )
            health_report['cross_layer_compatibility']['bronze_silver'] = compatibility

        if silver_schema and gold_schema:
            compatibility = self.schema_handler._check_layer_compatibility(
                silver_schema.schema, gold_schema.schema,
                MedallionLayer.SILVER, MedallionLayer.GOLD
            )
            health_report['cross_layer_compatibility']['silver_gold'] = compatibility

        # Generate recommendations
        recommendations = []
        for layer_name, layer_health in health_report['layers'].items():
            if layer_health['recent_changes'] > 10:
                recommendations.append(f"High schema activity in {layer_name} layer - consider stability review")
            if layer_health['compatibility_issues'] > 0:
                recommendations.append(f"Compatibility issues detected in {layer_name} layer")

        health_report['recommendations'] = recommendations

        return health_report


def create_schema_orchestrator(
    schema_registry_path: Path | None = None
) -> SchemaEvolutionOrchestrator:
    """Factory function to create a schema evolution orchestrator."""
    schema_handler = create_medallion_handler(schema_registry_path)
    return SchemaEvolutionOrchestrator(schema_handler)
