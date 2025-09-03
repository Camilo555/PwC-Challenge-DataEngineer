"""
Metadata Catalog Integrations

This module provides integrations between the Datadog metadata catalog
and various data platform components (ETL, ML, API, etc.).
"""

import inspect
from datetime import datetime
from typing import Any

import pandas as pd

from core.logging import get_logger
from core.metadata.datadog_metadata_catalog import (
    DataAssetMetadata,
    DataAssetType,
    DatadogMetadataCatalog,
    DataQualityLevel,
    get_metadata_catalog,
)

logger = get_logger(__name__)


class ETLMetadataCollector:
    """Collect metadata from ETL pipelines and register with catalog."""

    def __init__(self, catalog: DatadogMetadataCatalog = None):
        self.catalog = catalog or get_metadata_catalog()

    async def register_etl_pipeline(
        self,
        pipeline_name: str,
        source_tables: list[str],
        target_tables: list[str],
        transformation_logic: str = "",
        owner: str = "",
        team: str = "data_engineering"
    ) -> bool:
        """Register an ETL pipeline in the metadata catalog."""

        try:
            # Register pipeline as an asset
            pipeline_metadata = DataAssetMetadata(
                asset_id=f"pipeline_{pipeline_name}",
                name=pipeline_name,
                asset_type=DataAssetType.PIPELINE,
                description=f"ETL pipeline: {transformation_logic[:200]}",
                owner=owner,
                team=team,
                custom_properties={
                    "source_tables": source_tables,
                    "target_tables": target_tables,
                    "transformation_logic": transformation_logic
                },
                datadog_tags={
                    "pipeline_type": "etl",
                    "source_count": str(len(source_tables)),
                    "target_count": str(len(target_tables))
                }
            )

            # Register pipeline
            success = await self.catalog.register_asset(pipeline_metadata)

            if success:
                # Track lineage from source to target tables through pipeline
                for target_table in target_tables:
                    await self.catalog.track_lineage(
                        downstream_asset_id=target_table,
                        upstream_asset_ids=source_tables + [f"pipeline_{pipeline_name}"],
                        operation="etl_transform"
                    )

            return success

        except Exception as e:
            logger.error(f"Failed to register ETL pipeline {pipeline_name}: {e}")
            return False

    async def register_dataframe_transformation(
        self,
        input_df: pd.DataFrame,
        output_df: pd.DataFrame,
        transformation_name: str,
        source_asset_id: str = None,
        target_asset_id: str = None
    ) -> bool:
        """Register a dataframe transformation with automatic metadata extraction."""

        try:
            # Extract metadata from dataframes
            input_metadata = self._extract_dataframe_metadata(input_df, f"input_{transformation_name}")
            output_metadata = self._extract_dataframe_metadata(output_df, f"output_{transformation_name}")

            # Register input dataset if not exists
            if source_asset_id:
                input_metadata.asset_id = source_asset_id
            await self.catalog.register_asset(input_metadata)

            # Register output dataset
            if target_asset_id:
                output_metadata.asset_id = target_asset_id
            await self.catalog.register_asset(output_metadata)

            # Track lineage
            await self.catalog.track_lineage(
                downstream_asset_id=output_metadata.asset_id,
                upstream_asset_ids=[input_metadata.asset_id],
                operation="dataframe_transform"
            )

            return True

        except Exception as e:
            logger.error(f"Failed to register dataframe transformation {transformation_name}: {e}")
            return False

    def _extract_dataframe_metadata(self, df: pd.DataFrame, name: str) -> DataAssetMetadata:
        """Extract metadata from a pandas DataFrame."""

        # Analyze data quality
        null_percentage = df.isnull().sum().sum() / (len(df) * len(df.columns))
        quality_score = 1.0 - null_percentage

        if quality_score >= 0.9:
            quality_level = DataQualityLevel.EXCELLENT
        elif quality_score >= 0.8:
            quality_level = DataQualityLevel.GOOD
        elif quality_score >= 0.6:
            quality_level = DataQualityLevel.FAIR
        else:
            quality_level = DataQualityLevel.POOR

        # Extract schema
        schema = {
            "columns": [
                {
                    "name": col,
                    "type": str(df[col].dtype),
                    "null_count": int(df[col].isnull().sum()),
                    "unique_count": int(df[col].nunique())
                }
                for col in df.columns
            ]
        }

        # Detect potential PII fields
        pii_fields = set()
        for col in df.columns:
            col_lower = col.lower()
            if any(pii_term in col_lower for pii_term in ['email', 'ssn', 'phone', 'credit_card', 'password']):
                pii_fields.add(col)

        return DataAssetMetadata(
            asset_id=name,
            name=name,
            asset_type=DataAssetType.DATASET,
            schema=schema,
            row_count=len(df),
            size_bytes=df.memory_usage(deep=True).sum(),
            quality_level=quality_level,
            quality_score=quality_score,
            pii_fields=pii_fields,
            team="data_engineering"
        )


class MLMetadataCollector:
    """Collect metadata from ML pipelines and models."""

    def __init__(self, catalog: DatadogMetadataCatalog = None):
        self.catalog = catalog or get_metadata_catalog()

    async def register_ml_model(
        self,
        model_name: str,
        model_type: str,
        training_data_sources: list[str],
        features: list[str],
        target_variable: str = "",
        performance_metrics: dict[str, float] = None,
        owner: str = "",
        team: str = "ml_engineering"
    ) -> bool:
        """Register an ML model in the metadata catalog."""

        try:
            # Calculate model quality score based on performance metrics
            quality_score = 0.0
            if performance_metrics:
                # Use accuracy, f1_score, or similar metrics
                quality_score = max(
                    performance_metrics.get("accuracy", 0.0),
                    performance_metrics.get("f1_score", 0.0),
                    performance_metrics.get("r2_score", 0.0)
                )

            quality_level = DataQualityLevel.UNKNOWN
            if quality_score >= 0.9:
                quality_level = DataQualityLevel.EXCELLENT
            elif quality_score >= 0.8:
                quality_level = DataQualityLevel.GOOD
            elif quality_score >= 0.6:
                quality_level = DataQualityLevel.FAIR
            elif quality_score > 0:
                quality_level = DataQualityLevel.POOR

            model_metadata = DataAssetMetadata(
                asset_id=f"model_{model_name}",
                name=model_name,
                asset_type=DataAssetType.MODEL,
                description=f"{model_type} model for {target_variable}",
                owner=owner,
                team=team,
                quality_level=quality_level,
                quality_score=quality_score,
                custom_properties={
                    "model_type": model_type,
                    "features": features,
                    "target_variable": target_variable,
                    "performance_metrics": performance_metrics or {},
                    "training_data_sources": training_data_sources
                },
                datadog_tags={
                    "model_type": model_type,
                    "feature_count": str(len(features)),
                    "has_target": str(bool(target_variable))
                }
            )

            # Register model
            success = await self.catalog.register_asset(model_metadata)

            if success:
                # Track lineage from training data to model
                await self.catalog.track_lineage(
                    downstream_asset_id=f"model_{model_name}",
                    upstream_asset_ids=training_data_sources,
                    operation="ml_training"
                )

            return success

        except Exception as e:
            logger.error(f"Failed to register ML model {model_name}: {e}")
            return False

    async def update_model_performance(
        self,
        model_name: str,
        performance_metrics: dict[str, float],
        evaluation_data: dict[str, Any] = None
    ) -> bool:
        """Update model performance metrics."""

        try:
            # Calculate new quality score
            quality_score = max(
                performance_metrics.get("accuracy", 0.0),
                performance_metrics.get("f1_score", 0.0),
                performance_metrics.get("r2_score", 0.0)
            )

            quality_level = DataQualityLevel.UNKNOWN
            if quality_score >= 0.9:
                quality_level = DataQualityLevel.EXCELLENT
            elif quality_score >= 0.8:
                quality_level = DataQualityLevel.GOOD
            elif quality_score >= 0.6:
                quality_level = DataQualityLevel.FAIR
            elif quality_score > 0:
                quality_level = DataQualityLevel.POOR

            # Update quality
            success = await self.catalog.update_asset_quality(
                asset_id=f"model_{model_name}",
                quality_level=quality_level,
                quality_score=quality_score,
                quality_details={
                    "performance_metrics": performance_metrics,
                    "evaluation_data": evaluation_data,
                    "last_evaluation": datetime.utcnow().isoformat()
                }
            )

            return success

        except Exception as e:
            logger.error(f"Failed to update model performance for {model_name}: {e}")
            return False


class APIMetadataCollector:
    """Collect metadata from API endpoints."""

    def __init__(self, catalog: DatadogMetadataCatalog = None):
        self.catalog = catalog or get_metadata_catalog()

    async def register_api_endpoint(
        self,
        endpoint_path: str,
        method: str,
        handler_function: callable,
        data_sources: list[str] = None,
        response_schema: dict[str, Any] = None,
        owner: str = "",
        team: str = "api_engineering"
    ) -> bool:
        """Register an API endpoint in the metadata catalog."""

        try:
            # Extract metadata from function signature
            sig = inspect.signature(handler_function)
            parameters = [
                {
                    "name": param.name,
                    "type": str(param.annotation) if param.annotation != inspect.Parameter.empty else "unknown",
                    "required": param.default == inspect.Parameter.empty
                }
                for param in sig.parameters.values()
            ]

            endpoint_metadata = DataAssetMetadata(
                asset_id=f"api_{method}_{endpoint_path.replace('/', '_')}",
                name=f"{method} {endpoint_path}",
                asset_type=DataAssetType.API_ENDPOINT,
                description=f"API endpoint: {handler_function.__doc__ or 'No description'}",
                owner=owner,
                team=team,
                location=endpoint_path,
                schema={
                    "parameters": parameters,
                    "response_schema": response_schema
                },
                custom_properties={
                    "method": method,
                    "handler_function": handler_function.__name__,
                    "data_sources": data_sources or []
                },
                datadog_tags={
                    "http_method": method,
                    "endpoint_type": "rest_api",
                    "has_data_sources": str(bool(data_sources))
                }
            )

            # Register endpoint
            success = await self.catalog.register_asset(endpoint_metadata)

            if success and data_sources:
                # Track lineage from data sources to API endpoint
                await self.catalog.track_lineage(
                    downstream_asset_id=endpoint_metadata.asset_id,
                    upstream_asset_ids=data_sources,
                    operation="api_serve"
                )

            return success

        except Exception as e:
            logger.error(f"Failed to register API endpoint {method} {endpoint_path}: {e}")
            return False


class DatabaseMetadataCollector:
    """Collect metadata from database tables and views."""

    def __init__(self, catalog: DatadogMetadataCatalog = None):
        self.catalog = catalog or get_metadata_catalog()

    async def register_database_table(
        self,
        table_name: str,
        database_name: str,
        schema_name: str = "public",
        columns: list[dict[str, Any]] = None,
        row_count: int = None,
        table_size_bytes: int = None,
        owner: str = "",
        team: str = "data_engineering"
    ) -> bool:
        """Register a database table in the metadata catalog."""

        try:
            # Determine quality based on available metadata
            quality_score = 0.5  # Base score
            if columns:
                quality_score += 0.3  # Schema available
            if row_count is not None:
                quality_score += 0.1  # Row count available
            if table_size_bytes is not None:
                quality_score += 0.1  # Size information available

            quality_level = DataQualityLevel.FAIR
            if quality_score >= 0.8:
                quality_level = DataQualityLevel.GOOD
            elif quality_score >= 0.9:
                quality_level = DataQualityLevel.EXCELLENT

            # Detect potential PII columns
            pii_fields = set()
            if columns:
                for col in columns:
                    col_name = col.get("name", "").lower()
                    if any(pii_term in col_name for pii_term in ['email', 'ssn', 'phone', 'credit_card', 'password']):
                        pii_fields.add(col["name"])

            table_metadata = DataAssetMetadata(
                asset_id=f"table_{database_name}_{schema_name}_{table_name}",
                name=f"{database_name}.{schema_name}.{table_name}",
                asset_type=DataAssetType.TABLE,
                description=f"Database table in {database_name}",
                owner=owner,
                team=team,
                location=f"{database_name}.{schema_name}.{table_name}",
                schema={"columns": columns or []},
                row_count=row_count,
                size_bytes=table_size_bytes,
                quality_level=quality_level,
                quality_score=quality_score,
                pii_fields=pii_fields,
                custom_properties={
                    "database": database_name,
                    "schema": schema_name,
                    "table_type": "table"
                },
                datadog_tags={
                    "database": database_name,
                    "schema": schema_name,
                    "table_type": "table",
                    "has_pii": str(bool(pii_fields))
                }
            )

            return await self.catalog.register_asset(table_metadata)

        except Exception as e:
            logger.error(f"Failed to register database table {table_name}: {e}")
            return False


class MetadataCatalogOrchestrator:
    """Orchestrate metadata collection from multiple sources."""

    def __init__(self, catalog: DatadogMetadataCatalog = None):
        self.catalog = catalog or get_metadata_catalog()
        self.etl_collector = ETLMetadataCollector(catalog)
        self.ml_collector = MLMetadataCollector(catalog)
        self.api_collector = APIMetadataCollector(catalog)
        self.db_collector = DatabaseMetadataCollector(catalog)

    async def discover_and_register_assets(
        self,
        sources: list[str] = None
    ) -> dict[str, Any]:
        """Discover and register assets from multiple sources."""

        sources = sources or ["etl", "ml", "api", "database"]
        results = {
            "discovered_assets": 0,
            "registered_assets": 0,
            "failed_registrations": 0,
            "source_results": {}
        }

        try:
            if "etl" in sources:
                etl_results = await self._discover_etl_assets()
                results["source_results"]["etl"] = etl_results
                results["registered_assets"] += etl_results.get("registered", 0)
                results["failed_registrations"] += etl_results.get("failed", 0)

            if "ml" in sources:
                ml_results = await self._discover_ml_assets()
                results["source_results"]["ml"] = ml_results
                results["registered_assets"] += ml_results.get("registered", 0)
                results["failed_registrations"] += ml_results.get("failed", 0)

            if "api" in sources:
                api_results = await self._discover_api_assets()
                results["source_results"]["api"] = api_results
                results["registered_assets"] += api_results.get("registered", 0)
                results["failed_registrations"] += api_results.get("failed", 0)

            if "database" in sources:
                db_results = await self._discover_database_assets()
                results["source_results"]["database"] = db_results
                results["registered_assets"] += db_results.get("registered", 0)
                results["failed_registrations"] += db_results.get("failed", 0)

            results["discovered_assets"] = results["registered_assets"] + results["failed_registrations"]

            logger.info(f"Asset discovery completed: {results}")
            return results

        except Exception as e:
            logger.error(f"Asset discovery failed: {e}")
            return results

    async def _discover_etl_assets(self) -> dict[str, Any]:
        """Discover ETL assets (placeholder implementation)."""
        # In a real implementation, this would scan ETL configuration files,
        # pipeline definitions, etc.
        return {"discovered": 0, "registered": 0, "failed": 0}

    async def _discover_ml_assets(self) -> dict[str, Any]:
        """Discover ML assets (placeholder implementation)."""
        # In a real implementation, this would scan model registries,
        # training scripts, etc.
        return {"discovered": 0, "registered": 0, "failed": 0}

    async def _discover_api_assets(self) -> dict[str, Any]:
        """Discover API assets (placeholder implementation)."""
        # In a real implementation, this would scan FastAPI route definitions,
        # OpenAPI specs, etc.
        return {"discovered": 0, "registered": 0, "failed": 0}

    async def _discover_database_assets(self) -> dict[str, Any]:
        """Discover database assets (placeholder implementation)."""
        # In a real implementation, this would query database information_schema,
        # scan table definitions, etc.
        return {"discovered": 0, "registered": 0, "failed": 0}

    async def get_comprehensive_report(self) -> dict[str, Any]:
        """Get a comprehensive metadata catalog report."""
        health_metrics = await self.catalog.get_catalog_health()

        # Get lineage complexity metrics
        complex_lineages = []
        simple_lineages = []

        for asset_id, asset in self.catalog.catalog.items():
            depth = len(asset.upstream_dependencies)
            if depth > 3:
                complex_lineages.append(asset_id)
            else:
                simple_lineages.append(asset_id)

        report = {
            "catalog_health": health_metrics,
            "lineage_analysis": {
                "complex_lineages": len(complex_lineages),
                "simple_lineages": len(simple_lineages),
                "avg_lineage_depth": sum(
                    len(asset.upstream_dependencies)
                    for asset in self.catalog.catalog.values()
                ) / max(len(self.catalog.catalog), 1)
            },
            "compliance_summary": {
                "assets_with_pii": sum(
                    1 for asset in self.catalog.catalog.values()
                    if asset.pii_fields
                ),
                "assets_with_compliance_tags": sum(
                    1 for asset in self.catalog.catalog.values()
                    if asset.compliance_tags
                )
            },
            "quality_insights": {
                "high_quality_assets": sum(
                    1 for asset in self.catalog.catalog.values()
                    if asset.quality_score >= 0.8
                ),
                "needs_attention": sum(
                    1 for asset in self.catalog.catalog.values()
                    if asset.quality_score < 0.6
                )
            }
        }

        return report


# Global orchestrator instance
_orchestrator_instance: MetadataCatalogOrchestrator | None = None


def get_catalog_orchestrator() -> MetadataCatalogOrchestrator:
    """Get the global catalog orchestrator instance."""
    global _orchestrator_instance

    if _orchestrator_instance is None:
        _orchestrator_instance = MetadataCatalogOrchestrator()

    return _orchestrator_instance
