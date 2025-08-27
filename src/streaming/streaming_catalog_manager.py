"""
Real-time Data Cataloging and Metadata Management for Streaming
Provides comprehensive data cataloging, metadata management, and real-time data discovery for streaming data
"""
from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, expr, lit, struct, when,
    count, countDistinct, max as spark_max, min as spark_min,
    year, month, day, date_format
)
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import StructType, DataType

from core.config.unified_config import get_unified_config
from core.logging import get_logger
from etl.spark.delta_lake_manager import DeltaLakeManager
from monitoring.advanced_metrics import get_metrics_collector


class DataAssetType(Enum):
    """Types of data assets"""
    TABLE = "table"
    VIEW = "view"
    STREAM = "stream"
    TOPIC = "topic"
    FEATURE_SET = "feature_set"
    MODEL = "model"
    DASHBOARD = "dashboard"
    PIPELINE = "pipeline"


class DataDomain(Enum):
    """Business data domains"""
    SALES = "sales"
    CUSTOMER = "customer"
    PRODUCT = "product"
    FINANCE = "finance"
    MARKETING = "marketing"
    OPERATIONS = "operations"
    COMPLIANCE = "compliance"
    ANALYTICS = "analytics"


class SensitivityLevel(Enum):
    """Data sensitivity levels"""
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"
    HIGHLY_RESTRICTED = "highly_restricted"


class ProcessingLayer(Enum):
    """Data processing layers"""
    RAW = "raw"
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
    ANALYTICS = "analytics"
    SERVING = "serving"


@dataclass
class DataAssetMetadata:
    """Comprehensive metadata for data assets"""
    asset_id: str
    asset_name: str
    asset_type: DataAssetType
    domain: DataDomain
    processing_layer: ProcessingLayer
    
    # Technical metadata
    database_name: Optional[str] = None
    table_name: Optional[str] = None
    schema: Optional[StructType] = None
    location_path: Optional[str] = None
    format: Optional[str] = None
    partition_columns: List[str] = field(default_factory=list)
    
    # Business metadata
    description: str = ""
    business_owner: str = ""
    technical_owner: str = ""
    tags: Set[str] = field(default_factory=set)
    
    # Governance metadata
    sensitivity_level: SensitivityLevel = SensitivityLevel.INTERNAL
    retention_days: Optional[int] = None
    compliance_tags: Set[str] = field(default_factory=set)
    
    # Quality metadata
    quality_score: Optional[float] = None
    freshness_sla_minutes: Optional[int] = None
    
    # Usage metadata
    access_count: int = 0
    last_accessed: Optional[datetime] = None
    dependent_assets: Set[str] = field(default_factory=set)
    
    # Timestamps
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)


@dataclass
class SchemaChange:
    """Schema change tracking"""
    change_id: str
    asset_id: str
    change_type: str  # added, removed, modified, renamed
    column_name: str
    old_value: Optional[Any] = None
    new_value: Optional[Any] = None
    timestamp: datetime = field(default_factory=datetime.now)
    impact_assessment: Optional[str] = None


@dataclass
class DataLineage:
    """Data lineage information"""
    lineage_id: str
    source_asset_id: str
    target_asset_id: str
    transformation_type: str
    transformation_description: str
    processing_timestamp: datetime
    confidence_score: float = 1.0


class StreamingCatalogManager:
    """Real-time data catalog manager"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = get_logger(__name__)
        self.metrics_collector = get_metrics_collector()
        self.delta_manager = DeltaLakeManager(spark)
        
        # Catalog storage
        self.catalog_path = Path("./data/catalog")
        self.catalog_path.mkdir(parents=True, exist_ok=True)
        
        # In-memory catalog for fast access
        self.asset_catalog: Dict[str, DataAssetMetadata] = {}
        self.schema_changes: List[SchemaChange] = []
        self.lineage_graph: Dict[str, List[DataLineage]] = {}
        
        # Initialize catalog tables
        self._initialize_catalog_tables()
    
    def _initialize_catalog_tables(self):
        """Initialize Delta tables for catalog persistence"""
        try:
            # Create catalog tables if they don't exist
            catalog_tables = {
                "asset_metadata": self._get_asset_metadata_schema(),
                "schema_changes": self._get_schema_changes_schema(),
                "data_lineage": self._get_data_lineage_schema(),
                "usage_stats": self._get_usage_stats_schema()
            }
            
            for table_name, schema in catalog_tables.items():
                table_path = self.catalog_path / table_name
                
                if not (table_path.exists() and (table_path / "_delta_log").exists()):
                    # Create empty Delta table with schema
                    empty_df = self.spark.createDataFrame([], schema)
                    (
                        empty_df.write
                        .format("delta")
                        .mode("overwrite")
                        .save(str(table_path))
                    )
                    self.logger.info(f"Created catalog table: {table_name}")
            
            # Load existing catalog into memory
            self._load_catalog_from_storage()
            
        except Exception as e:
            self.logger.error(f"Failed to initialize catalog tables: {e}")
            raise
    
    def _get_asset_metadata_schema(self) -> StructType:
        """Get schema for asset metadata table"""
        from pyspark.sql.types import (
            StructType, StructField, StringType, IntegerType, 
            DoubleType, TimestampType, ArrayType, BooleanType
        )
        
        return StructType([
            StructField("asset_id", StringType(), False),
            StructField("asset_name", StringType(), False),
            StructField("asset_type", StringType(), False),
            StructField("domain", StringType(), False),
            StructField("processing_layer", StringType(), False),
            StructField("database_name", StringType(), True),
            StructField("table_name", StringType(), True),
            StructField("location_path", StringType(), True),
            StructField("format", StringType(), True),
            StructField("partition_columns", ArrayType(StringType()), True),
            StructField("description", StringType(), True),
            StructField("business_owner", StringType(), True),
            StructField("technical_owner", StringType(), True),
            StructField("tags", ArrayType(StringType()), True),
            StructField("sensitivity_level", StringType(), True),
            StructField("retention_days", IntegerType(), True),
            StructField("compliance_tags", ArrayType(StringType()), True),
            StructField("quality_score", DoubleType(), True),
            StructField("freshness_sla_minutes", IntegerType(), True),
            StructField("access_count", IntegerType(), True),
            StructField("last_accessed", TimestampType(), True),
            StructField("created_at", TimestampType(), False),
            StructField("updated_at", TimestampType(), False)
        ])
    
    def _get_schema_changes_schema(self) -> StructType:
        """Get schema for schema changes table"""
        from pyspark.sql.types import StructType, StructField, StringType, TimestampType
        
        return StructType([
            StructField("change_id", StringType(), False),
            StructField("asset_id", StringType(), False),
            StructField("change_type", StringType(), False),
            StructField("column_name", StringType(), False),
            StructField("old_value", StringType(), True),
            StructField("new_value", StringType(), True),
            StructField("timestamp", TimestampType(), False),
            StructField("impact_assessment", StringType(), True)
        ])
    
    def _get_data_lineage_schema(self) -> StructType:
        """Get schema for data lineage table"""
        from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType
        
        return StructType([
            StructField("lineage_id", StringType(), False),
            StructField("source_asset_id", StringType(), False),
            StructField("target_asset_id", StringType(), False),
            StructField("transformation_type", StringType(), False),
            StructField("transformation_description", StringType(), True),
            StructField("processing_timestamp", TimestampType(), False),
            StructField("confidence_score", DoubleType(), True)
        ])
    
    def _get_usage_stats_schema(self) -> StructType:
        """Get schema for usage statistics table"""
        from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType
        
        return StructType([
            StructField("usage_id", StringType(), False),
            StructField("asset_id", StringType(), False),
            StructField("user_id", StringType(), True),
            StructField("access_type", StringType(), False),
            StructField("access_timestamp", TimestampType(), False),
            StructField("records_accessed", IntegerType(), True)
        ])
    
    def _load_catalog_from_storage(self):
        """Load catalog from persistent storage into memory"""
        try:
            metadata_path = self.catalog_path / "asset_metadata"
            if metadata_path.exists():
                metadata_df = self.spark.read.format("delta").load(str(metadata_path))
                
                # Convert to in-memory catalog
                for row in metadata_df.collect():
                    asset_metadata = self._row_to_asset_metadata(row)
                    self.asset_catalog[asset_metadata.asset_id] = asset_metadata
                
                self.logger.info(f"Loaded {len(self.asset_catalog)} assets into catalog")
        
        except Exception as e:
            self.logger.error(f"Failed to load catalog from storage: {e}")
    
    def _row_to_asset_metadata(self, row) -> DataAssetMetadata:
        """Convert DataFrame row to DataAssetMetadata"""
        return DataAssetMetadata(
            asset_id=row["asset_id"],
            asset_name=row["asset_name"],
            asset_type=DataAssetType(row["asset_type"]),
            domain=DataDomain(row["domain"]),
            processing_layer=ProcessingLayer(row["processing_layer"]),
            database_name=row["database_name"],
            table_name=row["table_name"],
            location_path=row["location_path"],
            format=row["format"],
            partition_columns=row["partition_columns"] or [],
            description=row["description"] or "",
            business_owner=row["business_owner"] or "",
            technical_owner=row["technical_owner"] or "",
            tags=set(row["tags"] or []),
            sensitivity_level=SensitivityLevel(row["sensitivity_level"]),
            retention_days=row["retention_days"],
            compliance_tags=set(row["compliance_tags"] or []),
            quality_score=row["quality_score"],
            freshness_sla_minutes=row["freshness_sla_minutes"],
            access_count=row["access_count"],
            last_accessed=row["last_accessed"],
            created_at=row["created_at"],
            updated_at=row["updated_at"]
        )
    
    def register_streaming_asset(
        self,
        asset_name: str,
        asset_type: DataAssetType,
        domain: DataDomain,
        processing_layer: ProcessingLayer,
        schema: Optional[StructType] = None,
        location_path: Optional[str] = None,
        **metadata_kwargs
    ) -> str:
        """Register a new streaming data asset"""
        try:
            asset_id = str(uuid.uuid4())
            
            # Create metadata
            asset_metadata = DataAssetMetadata(
                asset_id=asset_id,
                asset_name=asset_name,
                asset_type=asset_type,
                domain=domain,
                processing_layer=processing_layer,
                schema=schema,
                location_path=location_path,
                **metadata_kwargs
            )
            
            # Store in memory catalog
            self.asset_catalog[asset_id] = asset_metadata
            
            # Persist to Delta table
            self._persist_asset_metadata(asset_metadata)
            
            self.logger.info(f"Registered streaming asset: {asset_name} ({asset_id})")
            return asset_id
            
        except Exception as e:
            self.logger.error(f"Failed to register streaming asset: {e}")
            raise
    
    def _persist_asset_metadata(self, metadata: DataAssetMetadata):
        """Persist asset metadata to Delta table"""
        try:
            # Convert to DataFrame row
            metadata_dict = {
                "asset_id": metadata.asset_id,
                "asset_name": metadata.asset_name,
                "asset_type": metadata.asset_type.value,
                "domain": metadata.domain.value,
                "processing_layer": metadata.processing_layer.value,
                "database_name": metadata.database_name,
                "table_name": metadata.table_name,
                "location_path": metadata.location_path,
                "format": metadata.format,
                "partition_columns": list(metadata.partition_columns),
                "description": metadata.description,
                "business_owner": metadata.business_owner,
                "technical_owner": metadata.technical_owner,
                "tags": list(metadata.tags),
                "sensitivity_level": metadata.sensitivity_level.value,
                "retention_days": metadata.retention_days,
                "compliance_tags": list(metadata.compliance_tags),
                "quality_score": metadata.quality_score,
                "freshness_sla_minutes": metadata.freshness_sla_minutes,
                "access_count": metadata.access_count,
                "last_accessed": metadata.last_accessed,
                "created_at": metadata.created_at,
                "updated_at": metadata.updated_at
            }
            
            # Create DataFrame and write to Delta
            df = self.spark.createDataFrame([metadata_dict], self._get_asset_metadata_schema())
            metadata_path = self.catalog_path / "asset_metadata"
            
            (
                df.write
                .format("delta")
                .mode("append")
                .save(str(metadata_path))
            )
            
        except Exception as e:
            self.logger.error(f"Failed to persist asset metadata: {e}")
    
    def track_schema_change(
        self,
        asset_id: str,
        old_schema: Optional[StructType],
        new_schema: StructType
    ) -> List[SchemaChange]:
        """Track schema changes for an asset"""
        try:
            changes = []
            
            if old_schema is None:
                # New asset - all fields are additions
                for field in new_schema.fields:
                    change = SchemaChange(
                        change_id=str(uuid.uuid4()),
                        asset_id=asset_id,
                        change_type="added",
                        column_name=field.name,
                        new_value=str(field.dataType),
                        impact_assessment="low"
                    )
                    changes.append(change)
            else:
                # Compare schemas
                old_fields = {f.name: f for f in old_schema.fields}
                new_fields = {f.name: f for f in new_schema.fields}
                
                # Check for added fields
                for field_name, field in new_fields.items():
                    if field_name not in old_fields:
                        change = SchemaChange(
                            change_id=str(uuid.uuid4()),
                            asset_id=asset_id,
                            change_type="added",
                            column_name=field_name,
                            new_value=str(field.dataType),
                            impact_assessment="low"
                        )
                        changes.append(change)
                
                # Check for removed fields
                for field_name, field in old_fields.items():
                    if field_name not in new_fields:
                        change = SchemaChange(
                            change_id=str(uuid.uuid4()),
                            asset_id=asset_id,
                            change_type="removed",
                            column_name=field_name,
                            old_value=str(field.dataType),
                            impact_assessment="high"
                        )
                        changes.append(change)
                
                # Check for modified fields
                for field_name, old_field in old_fields.items():
                    if field_name in new_fields:
                        new_field = new_fields[field_name]
                        if old_field.dataType != new_field.dataType:
                            change = SchemaChange(
                                change_id=str(uuid.uuid4()),
                                asset_id=asset_id,
                                change_type="modified",
                                column_name=field_name,
                                old_value=str(old_field.dataType),
                                new_value=str(new_field.dataType),
                                impact_assessment="medium"
                            )
                            changes.append(change)
            
            # Store schema changes
            if changes:
                self.schema_changes.extend(changes)
                self._persist_schema_changes(changes)
                
                self.logger.info(f"Tracked {len(changes)} schema changes for asset {asset_id}")
            
            return changes
            
        except Exception as e:
            self.logger.error(f"Failed to track schema changes: {e}")
            return []
    
    def _persist_schema_changes(self, changes: List[SchemaChange]):
        """Persist schema changes to Delta table"""
        try:
            changes_data = []
            for change in changes:
                changes_data.append({
                    "change_id": change.change_id,
                    "asset_id": change.asset_id,
                    "change_type": change.change_type,
                    "column_name": change.column_name,
                    "old_value": change.old_value,
                    "new_value": change.new_value,
                    "timestamp": change.timestamp,
                    "impact_assessment": change.impact_assessment
                })
            
            df = self.spark.createDataFrame(changes_data, self._get_schema_changes_schema())
            changes_path = self.catalog_path / "schema_changes"
            
            (
                df.write
                .format("delta")
                .mode("append")
                .save(str(changes_path))
            )
            
        except Exception as e:
            self.logger.error(f"Failed to persist schema changes: {e}")
    
    def record_data_lineage(
        self,
        source_asset_id: str,
        target_asset_id: str,
        transformation_type: str,
        transformation_description: str
    ) -> str:
        """Record data lineage between assets"""
        try:
            lineage_id = str(uuid.uuid4())
            
            lineage = DataLineage(
                lineage_id=lineage_id,
                source_asset_id=source_asset_id,
                target_asset_id=target_asset_id,
                transformation_type=transformation_type,
                transformation_description=transformation_description,
                processing_timestamp=datetime.now()
            )
            
            # Store in lineage graph
            if source_asset_id not in self.lineage_graph:
                self.lineage_graph[source_asset_id] = []
            self.lineage_graph[source_asset_id].append(lineage)
            
            # Persist lineage
            self._persist_data_lineage(lineage)
            
            self.logger.info(f"Recorded data lineage: {source_asset_id} -> {target_asset_id}")
            return lineage_id
            
        except Exception as e:
            self.logger.error(f"Failed to record data lineage: {e}")
            raise
    
    def _persist_data_lineage(self, lineage: DataLineage):
        """Persist data lineage to Delta table"""
        try:
            lineage_dict = {
                "lineage_id": lineage.lineage_id,
                "source_asset_id": lineage.source_asset_id,
                "target_asset_id": lineage.target_asset_id,
                "transformation_type": lineage.transformation_type,
                "transformation_description": lineage.transformation_description,
                "processing_timestamp": lineage.processing_timestamp,
                "confidence_score": lineage.confidence_score
            }
            
            df = self.spark.createDataFrame([lineage_dict], self._get_data_lineage_schema())
            lineage_path = self.catalog_path / "data_lineage"
            
            (
                df.write
                .format("delta")
                .mode("append")
                .save(str(lineage_path))
            )
            
        except Exception as e:
            self.logger.error(f"Failed to persist data lineage: {e}")
    
    def search_assets(
        self,
        query: str = "",
        asset_type: Optional[DataAssetType] = None,
        domain: Optional[DataDomain] = None,
        processing_layer: Optional[ProcessingLayer] = None,
        tags: Optional[Set[str]] = None
    ) -> List[DataAssetMetadata]:
        """Search for data assets"""
        try:
            results = []
            
            for asset in self.asset_catalog.values():
                # Text search
                if query and query.lower() not in asset.asset_name.lower() and query.lower() not in asset.description.lower():
                    continue
                
                # Type filter
                if asset_type and asset.asset_type != asset_type:
                    continue
                
                # Domain filter
                if domain and asset.domain != domain:
                    continue
                
                # Processing layer filter
                if processing_layer and asset.processing_layer != processing_layer:
                    continue
                
                # Tags filter
                if tags and not tags.intersection(asset.tags):
                    continue
                
                results.append(asset)
            
            self.logger.info(f"Found {len(results)} assets matching search criteria")
            return results
            
        except Exception as e:
            self.logger.error(f"Asset search failed: {e}")
            return []
    
    def get_asset_lineage(
        self,
        asset_id: str,
        direction: str = "downstream",
        depth: int = 3
    ) -> Dict[str, Any]:
        """Get data lineage for an asset"""
        try:
            lineage_info = {
                "asset_id": asset_id,
                "direction": direction,
                "depth": depth,
                "lineage": []
            }
            
            if direction == "downstream":
                # Find assets that depend on this asset
                lineage_info["lineage"] = self._get_downstream_lineage(asset_id, depth)
            elif direction == "upstream":
                # Find assets this asset depends on
                lineage_info["lineage"] = self._get_upstream_lineage(asset_id, depth)
            else:
                # Get both directions
                lineage_info["downstream"] = self._get_downstream_lineage(asset_id, depth)
                lineage_info["upstream"] = self._get_upstream_lineage(asset_id, depth)
            
            return lineage_info
            
        except Exception as e:
            self.logger.error(f"Failed to get asset lineage: {e}")
            return {"error": str(e)}
    
    def _get_downstream_lineage(self, asset_id: str, depth: int) -> List[Dict[str, Any]]:
        """Get downstream lineage"""
        if depth <= 0 or asset_id not in self.lineage_graph:
            return []
        
        downstream = []
        for lineage in self.lineage_graph[asset_id]:
            lineage_info = {
                "target_asset_id": lineage.target_asset_id,
                "transformation_type": lineage.transformation_type,
                "transformation_description": lineage.transformation_description,
                "processing_timestamp": lineage.processing_timestamp.isoformat(),
                "children": self._get_downstream_lineage(lineage.target_asset_id, depth - 1)
            }
            downstream.append(lineage_info)
        
        return downstream
    
    def _get_upstream_lineage(self, asset_id: str, depth: int) -> List[Dict[str, Any]]:
        """Get upstream lineage"""
        if depth <= 0:
            return []
        
        upstream = []
        # Find all lineage records where this asset is the target
        for source_id, lineages in self.lineage_graph.items():
            for lineage in lineages:
                if lineage.target_asset_id == asset_id:
                    lineage_info = {
                        "source_asset_id": source_id,
                        "transformation_type": lineage.transformation_type,
                        "transformation_description": lineage.transformation_description,
                        "processing_timestamp": lineage.processing_timestamp.isoformat(),
                        "parents": self._get_upstream_lineage(source_id, depth - 1)
                    }
                    upstream.append(lineage_info)
        
        return upstream
    
    def get_catalog_statistics(self) -> Dict[str, Any]:
        """Get catalog statistics"""
        try:
            # Asset type distribution
            asset_types = {}
            domain_distribution = {}
            layer_distribution = {}
            sensitivity_distribution = {}
            
            for asset in self.asset_catalog.values():
                # Asset types
                asset_type = asset.asset_type.value
                asset_types[asset_type] = asset_types.get(asset_type, 0) + 1
                
                # Domains
                domain = asset.domain.value
                domain_distribution[domain] = domain_distribution.get(domain, 0) + 1
                
                # Layers
                layer = asset.processing_layer.value
                layer_distribution[layer] = layer_distribution.get(layer, 0) + 1
                
                # Sensitivity
                sensitivity = asset.sensitivity_level.value
                sensitivity_distribution[sensitivity] = sensitivity_distribution.get(sensitivity, 0) + 1
            
            # Quality statistics
            quality_scores = [asset.quality_score for asset in self.asset_catalog.values() if asset.quality_score is not None]
            
            statistics = {
                "total_assets": len(self.asset_catalog),
                "asset_type_distribution": asset_types,
                "domain_distribution": domain_distribution,
                "layer_distribution": layer_distribution,
                "sensitivity_distribution": sensitivity_distribution,
                "schema_changes_tracked": len(self.schema_changes),
                "lineage_relationships": sum(len(lineages) for lineages in self.lineage_graph.values()),
                "quality_statistics": {
                    "assets_with_quality_scores": len(quality_scores),
                    "average_quality_score": sum(quality_scores) / len(quality_scores) if quality_scores else 0,
                    "min_quality_score": min(quality_scores) if quality_scores else 0,
                    "max_quality_score": max(quality_scores) if quality_scores else 0
                },
                "timestamp": datetime.now().isoformat()
            }
            
            return statistics
            
        except Exception as e:
            self.logger.error(f"Failed to get catalog statistics: {e}")
            return {"error": str(e)}
    
    def update_asset_metadata(
        self,
        asset_id: str,
        updates: Dict[str, Any]
    ) -> bool:
        """Update asset metadata"""
        try:
            if asset_id not in self.asset_catalog:
                self.logger.warning(f"Asset {asset_id} not found in catalog")
                return False
            
            asset = self.asset_catalog[asset_id]
            
            # Apply updates
            for field, value in updates.items():
                if hasattr(asset, field):
                    setattr(asset, field, value)
            
            # Update timestamp
            asset.updated_at = datetime.now()
            
            # Persist changes
            self._persist_asset_metadata(asset)
            
            self.logger.info(f"Updated metadata for asset {asset_id}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to update asset metadata: {e}")
            return False


# Factory function
def create_streaming_catalog_manager(spark: SparkSession) -> StreamingCatalogManager:
    """Create streaming catalog manager instance"""
    return StreamingCatalogManager(spark)


# Example usage
if __name__ == "__main__":
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
    
    # Create Spark session
    spark = (
        SparkSession.builder
        .appName("StreamingCatalogManager")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    
    try:
        print("Testing Streaming Catalog Manager...")
        
        # Create catalog manager
        catalog = create_streaming_catalog_manager(spark)
        
        # Register sample assets
        retail_schema = StructType([
            StructField("invoice_no", StringType(), True),
            StructField("quantity", IntegerType(), True),
            StructField("unit_price", DoubleType(), True)
        ])
        
        # Register Bronze layer asset
        bronze_asset_id = catalog.register_streaming_asset(
            asset_name="retail_transactions_bronze",
            asset_type=DataAssetType.TABLE,
            domain=DataDomain.SALES,
            processing_layer=ProcessingLayer.BRONZE,
            schema=retail_schema,
            location_path="./data/bronze/retail_transactions",
            description="Raw retail transaction data from streaming sources",
            business_owner="Sales Team",
            technical_owner="Data Engineering",
            tags={"retail", "transactions", "real-time"},
            sensitivity_level=SensitivityLevel.INTERNAL
        )
        
        # Register Silver layer asset
        silver_asset_id = catalog.register_streaming_asset(
            asset_name="retail_transactions_silver",
            asset_type=DataAssetType.TABLE,
            domain=DataDomain.SALES,
            processing_layer=ProcessingLayer.SILVER,
            description="Cleaned and enriched retail transaction data",
            business_owner="Sales Team",
            technical_owner="Data Engineering",
            tags={"retail", "transactions", "cleaned"}
        )
        
        # Record lineage
        catalog.record_data_lineage(
            source_asset_id=bronze_asset_id,
            target_asset_id=silver_asset_id,
            transformation_type="streaming_etl",
            transformation_description="Real-time data cleaning and enrichment"
        )
        
        # Search assets
        search_results = catalog.search_assets(
            query="retail",
            domain=DataDomain.SALES
        )
        
        print(f"✅ Registered {len(catalog.asset_catalog)} assets")
        print(f"   Search results: {len(search_results)}")
        
        # Get statistics
        stats = catalog.get_catalog_statistics()
        print(f"   Asset types: {stats['asset_type_distribution']}")
        print(f"   Domains: {stats['domain_distribution']}")
        print(f"   Layers: {stats['layer_distribution']}")
        
        print("✅ Streaming Catalog Manager testing completed")
        
    except Exception as e:
        print(f"❌ Testing failed: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()
