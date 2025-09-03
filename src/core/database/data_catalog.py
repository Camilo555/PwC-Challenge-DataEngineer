"""
Comprehensive Data Catalog and Metadata Management System

Enterprise-grade data catalog with automated discovery, classification,
lineage tracking, and governance capabilities.
"""

from __future__ import annotations

import json
import re
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

from pydantic import BaseModel, Field
from sqlalchemy import Engine, inspect, text

from core.logging import get_logger

logger = get_logger(__name__)


class DataClassification(str, Enum):
    """Data sensitivity classification levels"""

    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"
    PERSONAL_DATA = "personal_data"
    FINANCIAL_DATA = "financial_data"


class DataFormat(str, Enum):
    """Data format types"""

    TEXT = "text"
    NUMERIC = "numeric"
    DATE = "date"
    DATETIME = "datetime"
    BOOLEAN = "boolean"
    JSON = "json"
    UUID = "uuid"
    EMAIL = "email"
    PHONE = "phone"
    URL = "url"
    IP_ADDRESS = "ip_address"


class AssetType(str, Enum):
    """Data asset types"""

    TABLE = "table"
    VIEW = "view"
    MATERIALIZED_VIEW = "materialized_view"
    INDEX = "index"
    PROCEDURE = "procedure"
    FUNCTION = "function"
    TRIGGER = "trigger"


@dataclass
class DataColumn:
    """Data column metadata"""

    name: str
    data_type: str
    format: DataFormat
    classification: DataClassification
    is_nullable: bool
    is_primary_key: bool = False
    is_foreign_key: bool = False
    foreign_key_reference: str | None = None
    description: str | None = None
    business_name: str | None = None
    tags: set[str] = field(default_factory=set)
    quality_score: float = 1.0
    sample_values: list[Any] = field(default_factory=list)
    unique_count: int | None = None
    null_count: int | None = None
    min_length: int | None = None
    max_length: int | None = None
    pattern_analysis: dict[str, Any] = field(default_factory=dict)
    last_profiled: datetime | None = None


@dataclass
class DataAsset:
    """Data asset metadata"""

    name: str
    asset_type: AssetType
    schema_name: str
    database_name: str
    description: str | None = None
    business_name: str | None = None
    owner: str | None = None
    steward: str | None = None
    classification: DataClassification = DataClassification.INTERNAL
    tags: set[str] = field(default_factory=set)
    columns: list[DataColumn] = field(default_factory=list)
    row_count: int | None = None
    size_bytes: int | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    last_accessed: datetime | None = None
    access_frequency: int = 0
    quality_score: float = 1.0
    lineage_upstream: set[str] = field(default_factory=set)
    lineage_downstream: set[str] = field(default_factory=set)
    governance_policies: list[str] = field(default_factory=list)
    retention_policy: str | None = None
    compliance_tags: set[str] = field(default_factory=set)


class DataLineage(BaseModel):
    """Data lineage tracking"""

    asset_id: str
    upstream_assets: list[str] = Field(default_factory=list)
    downstream_assets: list[str] = Field(default_factory=list)
    transformation_logic: str | None = None
    transformation_type: str = "unknown"
    lineage_level: int = 0  # 0 = direct, 1+ = levels of separation
    discovered_at: datetime = Field(default_factory=datetime.utcnow)
    confidence_score: float = 1.0

    class Config:
        arbitrary_types_allowed = True


class BusinessGlossary(BaseModel):
    """Business glossary entry"""

    term_id: str
    name: str
    definition: str
    category: str
    synonyms: list[str] = Field(default_factory=list)
    related_terms: list[str] = Field(default_factory=list)
    data_assets: list[str] = Field(default_factory=list)
    owner: str | None = None
    approved_by: str | None = None
    approval_date: datetime | None = None
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class DataCatalog:
    """
    Comprehensive data catalog system with automated discovery and classification.

    Features:
    - Automated schema discovery
    - Data profiling and classification
    - Lineage tracking
    - Business glossary management
    - Data quality monitoring
    - Governance and compliance
    - Search and discovery
    - Impact analysis
    """

    def __init__(self, engine: Engine, catalog_path: str = "./catalog"):
        self.engine = engine
        self.catalog_path = Path(catalog_path)
        self.catalog_path.mkdir(parents=True, exist_ok=True)

        # Catalog data structures
        self.assets: dict[str, DataAsset] = {}
        self.lineage: dict[str, DataLineage] = {}
        self.business_glossary: dict[str, BusinessGlossary] = {}

        # Classification rules
        self.classification_rules = self._initialize_classification_rules()
        self.format_patterns = self._initialize_format_patterns()

        # Discovery configuration
        self.discovery_config = {
            "enable_profiling": True,
            "profiling_sample_size": 1000,
            "enable_pattern_analysis": True,
            "enable_lineage_discovery": True,
            "classification_threshold": 0.7,
        }

        logger.info("Data Catalog initialized")

    def _initialize_classification_rules(self) -> dict[str, dict[str, Any]]:
        """Initialize data classification rules"""
        return {
            DataClassification.PERSONAL_DATA: {
                "patterns": [
                    r".*name.*",
                    r".*email.*",
                    r".*phone.*",
                    r".*address.*",
                    r".*ssn.*",
                    r".*social.*",
                    r".*birth.*",
                    r".*age.*",
                ],
                "keywords": ["name", "email", "phone", "address", "personal", "private"],
            },
            DataClassification.FINANCIAL_DATA: {
                "patterns": [
                    r".*salary.*",
                    r".*income.*",
                    r".*credit.*",
                    r".*account.*",
                    r".*balance.*",
                    r".*payment.*",
                    r".*price.*",
                    r".*amount.*",
                ],
                "keywords": ["salary", "income", "financial", "money", "payment", "credit"],
            },
            DataClassification.CONFIDENTIAL: {
                "patterns": [
                    r".*password.*",
                    r".*secret.*",
                    r".*token.*",
                    r".*key.*",
                    r".*auth.*",
                    r".*credential.*",
                ],
                "keywords": ["password", "secret", "confidential", "internal", "auth"],
            },
        }

    def _initialize_format_patterns(self) -> dict[DataFormat, list[str]]:
        """Initialize data format detection patterns"""
        return {
            DataFormat.EMAIL: [r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$"],
            DataFormat.PHONE: [r"^\+?[\d\s\-\(\)]{7,15}$"],
            DataFormat.UUID: [r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"],
            DataFormat.URL: [r"^https?://.*"],
            DataFormat.IP_ADDRESS: [r"^\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}$"],
            DataFormat.DATE: [r"^\d{4}-\d{2}-\d{2}$", r"^\d{2}/\d{2}/\d{4}$"],
            DataFormat.DATETIME: [r"^\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2}"],
        }

    async def discover_schema(self, schema_names: list[str] | None = None) -> dict[str, Any]:
        """Automated schema discovery and cataloging"""
        logger.info("Starting automated schema discovery")

        discovery_start = time.time()
        discovery_stats = {
            "schemas_discovered": 0,
            "tables_discovered": 0,
            "columns_discovered": 0,
            "assets_classified": 0,
            "lineage_relationships": 0,
        }

        try:
            inspector = inspect(self.engine)
            schemas = schema_names or ["public", "main"]  # Default schemas

            for schema in schemas:
                try:
                    schema_tables = inspector.get_table_names(schema=schema)
                    discovery_stats["schemas_discovered"] += 1

                    for table_name in schema_tables:
                        asset = await self._discover_table(inspector, table_name, schema)
                        if asset:
                            asset_id = f"{schema}.{table_name}"
                            self.assets[asset_id] = asset
                            discovery_stats["tables_discovered"] += 1
                            discovery_stats["columns_discovered"] += len(asset.columns)

                            # Classify asset
                            if self._classify_asset(asset):
                                discovery_stats["assets_classified"] += 1

                    # Discover views
                    try:
                        view_names = inspector.get_view_names(schema=schema)
                        for view_name in view_names:
                            asset = await self._discover_view(inspector, view_name, schema)
                            if asset:
                                asset_id = f"{schema}.{view_name}"
                                self.assets[asset_id] = asset
                                discovery_stats["tables_discovered"] += 1
                    except Exception as e:
                        logger.debug(f"View discovery failed for schema {schema}: {e}")

                except Exception as e:
                    logger.warning(f"Schema discovery failed for {schema}: {e}")

            # Discover lineage relationships
            if self.discovery_config["enable_lineage_discovery"]:
                lineage_count = await self._discover_lineage()
                discovery_stats["lineage_relationships"] = lineage_count

            # Save catalog
            await self._save_catalog()

            discovery_time = time.time() - discovery_start

            logger.info(
                f"Schema discovery completed in {discovery_time:.2f}s: "
                f"{discovery_stats['tables_discovered']} assets discovered"
            )

            return {
                "discovery_stats": discovery_stats,
                "discovery_time_seconds": discovery_time,
                "timestamp": datetime.utcnow().isoformat(),
            }

        except Exception as e:
            logger.error(f"Schema discovery failed: {e}")
            raise

    async def _discover_table(self, inspector, table_name: str, schema: str) -> DataAsset | None:
        """Discover and profile a database table"""
        try:
            # Get table columns
            columns_info = inspector.get_columns(table_name, schema=schema)
            foreign_keys = inspector.get_foreign_keys(table_name, schema=schema)
            primary_key = inspector.get_pk_constraint(table_name, schema=schema)

            # Create asset
            asset = DataAsset(
                name=table_name,
                asset_type=AssetType.TABLE,
                schema_name=schema,
                database_name=self.engine.url.database or "main",
                columns=[],
            )

            # Process columns
            pk_columns = set(primary_key.get("constrained_columns", []))
            fk_map = {
                fk["constrained_columns"][0]: fk for fk in foreign_keys if fk["constrained_columns"]
            }

            for col_info in columns_info:
                column = DataColumn(
                    name=col_info["name"],
                    data_type=str(col_info["type"]),
                    format=self._detect_data_format(col_info),
                    classification=DataClassification.INTERNAL,  # Default
                    is_nullable=col_info.get("nullable", True),
                    is_primary_key=col_info["name"] in pk_columns,
                    is_foreign_key=col_info["name"] in fk_map,
                )

                if column.is_foreign_key:
                    fk_info = fk_map[column.name]
                    column.foreign_key_reference = (
                        f"{fk_info['referred_table']}.{fk_info['referred_columns'][0]}"
                    )

                # Classify column
                column.classification = self._classify_column(column)

                # Profile column if enabled
                if self.discovery_config["enable_profiling"]:
                    await self._profile_column(asset, column)

                asset.columns.append(column)

            # Get table statistics
            await self._get_table_statistics(asset)

            return asset

        except Exception as e:
            logger.error(f"Table discovery failed for {schema}.{table_name}: {e}")
            return None

    async def _discover_view(self, inspector, view_name: str, schema: str) -> DataAsset | None:
        """Discover a database view"""
        try:
            # Get view definition
            try:
                inspector.get_view_definition(view_name, schema=schema)
            except Exception:
                pass

            # Create asset (simplified for views)
            asset = DataAsset(
                name=view_name,
                asset_type=AssetType.VIEW,
                schema_name=schema,
                database_name=self.engine.url.database or "main",
                description=f"Database view: {view_name}",
            )

            # Try to get column information
            try:
                columns_info = inspector.get_columns(view_name, schema=schema)
                for col_info in columns_info:
                    column = DataColumn(
                        name=col_info["name"],
                        data_type=str(col_info["type"]),
                        format=self._detect_data_format(col_info),
                        classification=DataClassification.INTERNAL,
                        is_nullable=col_info.get("nullable", True),
                    )
                    asset.columns.append(column)
            except Exception as e:
                logger.debug(f"Could not get column info for view {view_name}: {e}")

            return asset

        except Exception as e:
            logger.error(f"View discovery failed for {schema}.{view_name}: {e}")
            return None

    async def _profile_column(self, asset: DataAsset, column: DataColumn) -> None:
        """Profile column data for patterns and statistics"""
        try:
            table_ref = f"{asset.schema_name}.{asset.name}"
            sample_size = self.discovery_config["profiling_sample_size"]

            with self.engine.connect() as conn:
                # Get sample data for analysis
                sample_query = text(f"""
                    SELECT {column.name}
                    FROM {table_ref}
                    WHERE {column.name} IS NOT NULL
                    LIMIT {sample_size}
                """)

                result = conn.execute(sample_query)
                sample_values = [row[0] for row in result.fetchall()]

                if sample_values:
                    column.sample_values = sample_values[:10]  # Store first 10 samples

                    # Calculate statistics
                    column.unique_count = len(set(sample_values))

                    # String length analysis for text columns
                    if column.data_type.lower() in ["varchar", "text", "char"]:
                        lengths = [len(str(val)) for val in sample_values if val is not None]
                        if lengths:
                            column.min_length = min(lengths)
                            column.max_length = max(lengths)

                    # Pattern analysis
                    if self.discovery_config["enable_pattern_analysis"]:
                        column.pattern_analysis = self._analyze_patterns(sample_values)

                        # Update format based on pattern analysis
                        detected_format = self._detect_format_from_patterns(column.pattern_analysis)
                        if detected_format:
                            column.format = detected_format

                # Get null count
                null_query = text(f"""
                    SELECT COUNT(*)
                    FROM {table_ref}
                    WHERE {column.name} IS NULL
                """)
                null_result = conn.execute(null_query)
                column.null_count = null_result.scalar() or 0

                column.last_profiled = datetime.utcnow()

        except Exception as e:
            logger.debug(f"Column profiling failed for {column.name}: {e}")

    async def _get_table_statistics(self, asset: DataAsset) -> None:
        """Get table-level statistics"""
        try:
            table_ref = f"{asset.schema_name}.{asset.name}"

            with self.engine.connect() as conn:
                # Get row count
                count_query = text(f"SELECT COUNT(*) FROM {table_ref}")
                result = conn.execute(count_query)
                asset.row_count = result.scalar() or 0

                # Try to get table size (database-specific)
                if "postgresql" in str(self.engine.url).lower():
                    size_query = text(f"""
                        SELECT pg_total_relation_size('{table_ref}')
                    """)
                    try:
                        result = conn.execute(size_query)
                        asset.size_bytes = result.scalar()
                    except Exception:
                        pass

        except Exception as e:
            logger.debug(f"Table statistics failed for {asset.name}: {e}")

    async def _discover_lineage(self) -> int:
        """Discover data lineage relationships"""
        lineage_count = 0

        try:
            # Analyze foreign key relationships for lineage
            for asset_id, asset in self.assets.items():
                lineage = DataLineage(asset_id=asset_id)

                for column in asset.columns:
                    if column.is_foreign_key and column.foreign_key_reference:
                        # Add upstream relationship
                        ref_parts = column.foreign_key_reference.split(".")
                        if len(ref_parts) >= 2:
                            upstream_table = f"{asset.schema_name}.{ref_parts[0]}"
                            if upstream_table in self.assets:
                                lineage.upstream_assets.append(upstream_table)

                                # Add downstream to referenced table
                                if upstream_table not in self.lineage:
                                    self.lineage[upstream_table] = DataLineage(
                                        asset_id=upstream_table
                                    )
                                self.lineage[upstream_table].downstream_assets.append(asset_id)

                if lineage.upstream_assets or lineage.downstream_assets:
                    self.lineage[asset_id] = lineage
                    lineage_count += 1

            # TODO: Analyze query logs for additional lineage relationships
            # This would involve parsing SQL queries to find table relationships

        except Exception as e:
            logger.warning(f"Lineage discovery failed: {e}")

        return lineage_count

    def _classify_asset(self, asset: DataAsset) -> bool:
        """Classify asset based on content and metadata"""
        try:
            classification_scores = defaultdict(float)

            # Analyze asset name
            asset_name_lower = asset.name.lower()
            for classification, rules in self.classification_rules.items():
                for pattern in rules["patterns"]:
                    if re.search(pattern, asset_name_lower):
                        classification_scores[classification] += 0.3

                for keyword in rules["keywords"]:
                    if keyword in asset_name_lower:
                        classification_scores[classification] += 0.2

            # Analyze columns
            for column in asset.columns:
                column_classification = self._classify_column(column)
                if column_classification != DataClassification.INTERNAL:
                    classification_scores[column_classification] += 0.5

            # Apply highest scoring classification if above threshold
            if classification_scores:
                best_classification = max(classification_scores, key=classification_scores.get)
                if (
                    classification_scores[best_classification]
                    >= self.discovery_config["classification_threshold"]
                ):
                    asset.classification = best_classification
                    return True

            return False

        except Exception as e:
            logger.debug(f"Asset classification failed for {asset.name}: {e}")
            return False

    def _classify_column(self, column: DataColumn) -> DataClassification:
        """Classify individual column based on name and data"""
        try:
            classification_scores = defaultdict(float)
            column_name_lower = column.name.lower()

            # Check against classification rules
            for classification, rules in self.classification_rules.items():
                for pattern in rules["patterns"]:
                    if re.search(pattern, column_name_lower):
                        classification_scores[classification] += 0.7

                for keyword in rules["keywords"]:
                    if keyword in column_name_lower:
                        classification_scores[classification] += 0.5

            # Analyze sample data patterns if available
            if hasattr(column, "pattern_analysis") and column.pattern_analysis:
                if column.format == DataFormat.EMAIL:
                    classification_scores[DataClassification.PERSONAL_DATA] += 0.8
                elif column.format == DataFormat.PHONE:
                    classification_scores[DataClassification.PERSONAL_DATA] += 0.8

            # Return highest scoring classification if above threshold
            if classification_scores:
                best_classification = max(classification_scores, key=classification_scores.get)
                if (
                    classification_scores[best_classification]
                    >= self.discovery_config["classification_threshold"]
                ):
                    return best_classification

            return DataClassification.INTERNAL

        except Exception as e:
            logger.debug(f"Column classification failed for {column.name}: {e}")
            return DataClassification.INTERNAL

    def _detect_data_format(self, column_info: dict) -> DataFormat:
        """Detect data format from column metadata"""
        data_type = str(column_info["type"]).lower()

        if "int" in data_type or "numeric" in data_type or "decimal" in data_type:
            return DataFormat.NUMERIC
        elif "bool" in data_type:
            return DataFormat.BOOLEAN
        elif "date" in data_type and "time" not in data_type:
            return DataFormat.DATE
        elif "timestamp" in data_type or "datetime" in data_type:
            return DataFormat.DATETIME
        elif "json" in data_type:
            return DataFormat.JSON
        else:
            return DataFormat.TEXT

    def _analyze_patterns(self, sample_values: list[Any]) -> dict[str, Any]:
        """Analyze patterns in sample data"""
        patterns = {
            "total_samples": len(sample_values),
            "unique_samples": len(set(sample_values)),
            "format_matches": {},
            "common_patterns": [],
        }

        string_values = [str(val) for val in sample_values if val is not None]

        if string_values:
            # Test against format patterns
            for data_format, format_patterns in self.format_patterns.items():
                matches = 0
                for value in string_values[:100]:  # Test first 100 values
                    for pattern in format_patterns:
                        if re.match(pattern, str(value), re.IGNORECASE):
                            matches += 1
                            break

                match_ratio = matches / min(len(string_values), 100)
                if match_ratio > 0:
                    patterns["format_matches"][data_format.value] = match_ratio

            # Analyze common string patterns
            pattern_counts = defaultdict(int)
            for value in string_values[:50]:  # Analyze first 50 values
                # Simple pattern analysis
                pattern = re.sub(r"\d", "9", str(value))  # Replace digits with 9
                pattern = re.sub(r"[a-zA-Z]", "A", pattern)  # Replace letters with A
                pattern_counts[pattern] += 1

            # Store most common patterns
            patterns["common_patterns"] = [
                {"pattern": pattern, "count": count}
                for pattern, count in sorted(
                    pattern_counts.items(), key=lambda x: x[1], reverse=True
                )[:5]
            ]

        return patterns

    def _detect_format_from_patterns(self, pattern_analysis: dict[str, Any]) -> DataFormat | None:
        """Detect data format from pattern analysis"""
        format_matches = pattern_analysis.get("format_matches", {})

        # Find format with highest match ratio above threshold
        best_match = None
        best_ratio = 0.5  # Minimum threshold

        for format_name, ratio in format_matches.items():
            if ratio > best_ratio:
                best_ratio = ratio
                best_match = format_name

        if best_match:
            try:
                return DataFormat(best_match)
            except ValueError:
                pass

        return None

    def search_catalog(self, query: str, filters: dict[str, Any] | None = None) -> dict[str, Any]:
        """Search data catalog with filters"""
        results = {
            "assets": [],
            "columns": [],
            "business_terms": [],
            "total_results": 0,
            "query": query,
            "filters_applied": filters or {},
        }

        query_lower = query.lower()

        # Search assets
        for asset_id, asset in self.assets.items():
            score = 0

            # Name matching
            if query_lower in asset.name.lower():
                score += 10
            if asset.business_name and query_lower in asset.business_name.lower():
                score += 8
            if asset.description and query_lower in asset.description.lower():
                score += 5

            # Tag matching
            for tag in asset.tags:
                if query_lower in tag.lower():
                    score += 3

            # Apply filters
            if filters:
                if "asset_type" in filters and asset.asset_type.value not in filters["asset_type"]:
                    continue
                if (
                    "classification" in filters
                    and asset.classification.value not in filters["classification"]
                ):
                    continue
                if "schema" in filters and asset.schema_name not in filters["schema"]:
                    continue

            if score > 0:
                results["assets"].append(
                    {
                        "asset_id": asset_id,
                        "name": asset.name,
                        "asset_type": asset.asset_type.value,
                        "schema_name": asset.schema_name,
                        "classification": asset.classification.value,
                        "description": asset.description,
                        "match_score": score,
                    }
                )

        # Search columns
        for asset_id, asset in self.assets.items():
            for column in asset.columns:
                score = 0

                if query_lower in column.name.lower():
                    score += 10
                if column.business_name and query_lower in column.business_name.lower():
                    score += 8
                if column.description and query_lower in column.description.lower():
                    score += 5

                if score > 0:
                    results["columns"].append(
                        {
                            "asset_id": asset_id,
                            "column_name": column.name,
                            "data_type": column.data_type,
                            "classification": column.classification.value,
                            "description": column.description,
                            "match_score": score,
                        }
                    )

        # Search business glossary
        for term_id, term in self.business_glossary.items():
            score = 0

            if query_lower in term.name.lower():
                score += 10
            if query_lower in term.definition.lower():
                score += 5

            for synonym in term.synonyms:
                if query_lower in synonym.lower():
                    score += 8

            if score > 0:
                results["business_terms"].append(
                    {
                        "term_id": term_id,
                        "name": term.name,
                        "definition": term.definition,
                        "category": term.category,
                        "match_score": score,
                    }
                )

        # Sort results by score
        results["assets"].sort(key=lambda x: x["match_score"], reverse=True)
        results["columns"].sort(key=lambda x: x["match_score"], reverse=True)
        results["business_terms"].sort(key=lambda x: x["match_score"], reverse=True)

        results["total_results"] = (
            len(results["assets"]) + len(results["columns"]) + len(results["business_terms"])
        )

        return results

    def get_lineage_graph(self, asset_id: str, depth: int = 3) -> dict[str, Any]:
        """Get lineage graph for an asset"""
        graph = {"nodes": [], "edges": [], "root_asset": asset_id}

        visited = set()

        def _traverse_lineage(current_asset_id: str, current_depth: int, direction: str):
            if current_depth > depth or current_asset_id in visited:
                return

            visited.add(current_asset_id)

            # Add node
            if current_asset_id in self.assets:
                asset = self.assets[current_asset_id]
                graph["nodes"].append(
                    {
                        "id": current_asset_id,
                        "name": asset.name,
                        "asset_type": asset.asset_type.value,
                        "classification": asset.classification.value,
                        "depth": current_depth,
                    }
                )

            # Get lineage relationships
            if current_asset_id in self.lineage:
                lineage = self.lineage[current_asset_id]

                # Traverse upstream
                if direction in ["upstream", "both"]:
                    for upstream_id in lineage.upstream_assets:
                        graph["edges"].append(
                            {"from": upstream_id, "to": current_asset_id, "type": "upstream"}
                        )
                        _traverse_lineage(upstream_id, current_depth + 1, "upstream")

                # Traverse downstream
                if direction in ["downstream", "both"]:
                    for downstream_id in lineage.downstream_assets:
                        graph["edges"].append(
                            {"from": current_asset_id, "to": downstream_id, "type": "downstream"}
                        )
                        _traverse_lineage(downstream_id, current_depth + 1, "downstream")

        _traverse_lineage(asset_id, 0, "both")

        return graph

    def get_impact_analysis(self, asset_id: str) -> dict[str, Any]:
        """Perform impact analysis for changes to an asset"""
        if asset_id not in self.assets:
            return {"error": f"Asset {asset_id} not found"}

        # Get downstream dependencies
        lineage_graph = self.get_lineage_graph(asset_id, depth=5)
        downstream_assets = [node for node in lineage_graph["nodes"] if node["id"] != asset_id]

        # Analyze impact levels
        impact_levels = defaultdict(list)
        for edge in lineage_graph["edges"]:
            if edge["from"] == asset_id:
                impact_levels["direct"].append(edge["to"])
            elif edge["type"] == "downstream":
                impact_levels["indirect"].append(edge["to"])

        return {
            "asset_id": asset_id,
            "total_downstream_assets": len(downstream_assets),
            "direct_impact_count": len(impact_levels["direct"]),
            "indirect_impact_count": len(impact_levels["indirect"]),
            "impact_levels": dict(impact_levels),
            "affected_assets": downstream_assets,
            "recommendations": self._generate_impact_recommendations(asset_id, downstream_assets),
        }

    def _generate_impact_recommendations(
        self, asset_id: str, affected_assets: list[dict]
    ) -> list[str]:
        """Generate recommendations for impact analysis"""
        recommendations = []

        if len(affected_assets) > 10:
            recommendations.append("High impact change - coordinate with downstream asset owners")

        if any(asset.get("classification") == "critical" for asset in affected_assets):
            recommendations.append("Critical assets affected - perform thorough testing")

        recommendations.append("Review and update data lineage documentation")
        recommendations.append("Notify downstream consumers of planned changes")

        return recommendations

    async def _save_catalog(self) -> None:
        """Save catalog metadata to disk"""
        try:
            catalog_file = self.catalog_path / "catalog_metadata.json"

            catalog_data = {
                "assets": {
                    asset_id: {
                        "name": asset.name,
                        "asset_type": asset.asset_type.value,
                        "schema_name": asset.schema_name,
                        "database_name": asset.database_name,
                        "classification": asset.classification.value,
                        "description": asset.description,
                        "owner": asset.owner,
                        "tags": list(asset.tags),
                        "row_count": asset.row_count,
                        "columns": [
                            {
                                "name": col.name,
                                "data_type": col.data_type,
                                "format": col.format.value,
                                "classification": col.classification.value,
                                "is_nullable": col.is_nullable,
                                "is_primary_key": col.is_primary_key,
                                "is_foreign_key": col.is_foreign_key,
                                "description": col.description,
                            }
                            for col in asset.columns
                        ],
                    }
                    for asset_id, asset in self.assets.items()
                },
                "lineage": {
                    asset_id: {
                        "upstream_assets": lineage.upstream_assets,
                        "downstream_assets": lineage.downstream_assets,
                        "transformation_type": lineage.transformation_type,
                        "confidence_score": lineage.confidence_score,
                    }
                    for asset_id, lineage in self.lineage.items()
                },
                "metadata": {
                    "last_updated": datetime.utcnow().isoformat(),
                    "total_assets": len(self.assets),
                    "total_lineage_relationships": len(self.lineage),
                },
            }

            with open(catalog_file, "w") as f:
                json.dump(catalog_data, f, indent=2)

            logger.debug(f"Catalog saved to {catalog_file}")

        except Exception as e:
            logger.error(f"Failed to save catalog: {e}")

    def get_catalog_summary(self) -> dict[str, Any]:
        """Get comprehensive catalog summary"""
        asset_types = defaultdict(int)
        classifications = defaultdict(int)

        for asset in self.assets.values():
            asset_types[asset.asset_type.value] += 1
            classifications[asset.classification.value] += 1

        return {
            "total_assets": len(self.assets),
            "total_lineage_relationships": len(self.lineage),
            "total_business_terms": len(self.business_glossary),
            "asset_types": dict(asset_types),
            "classifications": dict(classifications),
            "discovery_config": self.discovery_config,
            "last_discovery": datetime.utcnow().isoformat(),  # Would track actual last discovery
        }


# Factory function
def create_data_catalog(engine: Engine, catalog_path: str = "./catalog") -> DataCatalog:
    """Create data catalog instance"""
    return DataCatalog(engine, catalog_path)
