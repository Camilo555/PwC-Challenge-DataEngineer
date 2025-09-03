"""
Datadog-Integrated Metadata Catalog

This module provides a centralized metadata catalog that integrates with Datadog
for monitoring, observability, and metadata management across the data platform.
"""

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

try:
    from datadog import api, initialize, statsd
    from datadog.api import Event, Metric
    DATADOG_AVAILABLE = True
except ImportError:
    # Mock Datadog functionality for demo purposes
    DATADOG_AVAILABLE = False

    class MockStatsd:
        def increment(self, metric, tags=None): pass
        def gauge(self, metric, value, tags=None): pass
        def histogram(self, metric, value, tags=None): pass

    class MockAPI:
        def post(self, data): return {"status": "ok"}

    statsd = MockStatsd()
    api = MockAPI()

    def initialize(**kwargs): pass

from core.logging import get_logger

# Handle settings import gracefully
try:
    from core.config import settings
except ImportError:
    # Create a mock settings object for demo purposes
    class MockSettings:
        datadog_api_key = None
        datadog_app_key = None

    settings = MockSettings()

logger = get_logger(__name__)


class DataAssetType(str, Enum):
    """Types of data assets in the catalog."""
    TABLE = "table"
    VIEW = "view"
    MODEL = "model"
    DATASET = "dataset"
    API_ENDPOINT = "api_endpoint"
    PIPELINE = "pipeline"
    DASHBOARD = "dashboard"
    REPORT = "report"
    STREAM = "stream"
    FILE = "file"


class DataQualityLevel(str, Enum):
    """Data quality levels."""
    EXCELLENT = "excellent"
    GOOD = "good"
    FAIR = "fair"
    POOR = "poor"
    UNKNOWN = "unknown"


class MetadataChangeType(str, Enum):
    """Types of metadata changes."""
    CREATED = "created"
    UPDATED = "updated"
    DELETED = "deleted"
    SCHEMA_CHANGED = "schema_changed"
    ACCESS_CHANGED = "access_changed"
    QUALITY_CHANGED = "quality_changed"


@dataclass
class DataAssetMetadata:
    """Metadata for a data asset."""
    asset_id: str
    name: str
    asset_type: DataAssetType
    description: str = ""
    owner: str = ""
    team: str = ""
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)

    # Technical metadata
    schema: dict[str, Any] = field(default_factory=dict)
    location: str = ""
    size_bytes: int | None = None
    row_count: int | None = None

    # Quality metadata
    quality_level: DataQualityLevel = DataQualityLevel.UNKNOWN
    quality_score: float = 0.0
    last_quality_check: datetime | None = None

    # Lineage and dependencies
    upstream_dependencies: set[str] = field(default_factory=set)
    downstream_dependencies: set[str] = field(default_factory=set)

    # Access and governance
    access_level: str = "restricted"
    compliance_tags: set[str] = field(default_factory=set)
    pii_fields: set[str] = field(default_factory=set)

    # Custom metadata
    custom_properties: dict[str, Any] = field(default_factory=dict)

    # Datadog tags for monitoring
    datadog_tags: dict[str, str] = field(default_factory=dict)


@dataclass
class MetadataChangeEvent:
    """Event for metadata changes."""
    asset_id: str
    change_type: MetadataChangeType
    changed_fields: list[str]
    old_values: dict[str, Any]
    new_values: dict[str, Any]
    changed_by: str
    timestamp: datetime = field(default_factory=datetime.utcnow)
    change_reason: str = ""


class DatadogMetadataCatalog:
    """
    Centralized metadata catalog with Datadog integration.

    Features:
    - Asset metadata management
    - Data lineage tracking
    - Quality monitoring with Datadog metrics
    - Change tracking and notifications
    - Search and discovery
    - Compliance monitoring
    """

    def __init__(
        self,
        datadog_api_key: str = None,
        datadog_app_key: str = None,
        statsd_host: str = "localhost",
        statsd_port: int = 8125,
        environment: str = "development"
    ):
        """Initialize the metadata catalog with Datadog integration."""

        # Initialize Datadog
        self.api_key = datadog_api_key or getattr(settings, 'datadog_api_key', None)
        self.app_key = datadog_app_key or getattr(settings, 'datadog_app_key', None)
        self.environment = environment

        if DATADOG_AVAILABLE and self.api_key and self.app_key:
            initialize(api_key=self.api_key, app_key=self.app_key)
            self.datadog_enabled = True
            logger.info("Datadog integration enabled")
        else:
            self.datadog_enabled = False
            logger.warning("Datadog integration disabled (credentials not provided or library not available)")

        # Use the statsd client from datadog module or mock
        self.statsd = statsd

        # In-memory catalog (in production, would use persistent storage)
        self.catalog: dict[str, DataAssetMetadata] = {}
        self.change_history: list[MetadataChangeEvent] = []

        # Monitoring state
        self.last_health_check = datetime.utcnow()
        self.catalog_metrics = {
            "total_assets": 0,
            "quality_scores": {},
            "lineage_depth": {}
        }

    async def register_asset(self, metadata: DataAssetMetadata) -> bool:
        """Register a new data asset in the catalog."""
        try:
            # Generate asset ID if not provided
            if not metadata.asset_id:
                metadata.asset_id = self._generate_asset_id(metadata)

            # Set Datadog tags
            metadata.datadog_tags.update({
                "asset_type": metadata.asset_type.value,
                "environment": self.environment,
                "team": metadata.team,
                "owner": metadata.owner,
                "quality_level": metadata.quality_level.value
            })

            # Store in catalog
            is_new = metadata.asset_id not in self.catalog
            self.catalog[metadata.asset_id] = metadata

            # Record change event
            change_type = MetadataChangeType.CREATED if is_new else MetadataChangeType.UPDATED
            await self._record_change_event(
                metadata.asset_id,
                change_type,
                changed_fields=["all"] if is_new else ["updated_at"],
                new_values={"metadata": metadata.__dict__},
                changed_by="system"
            )

            # Send metrics to Datadog
            await self._send_asset_metrics(metadata, "registered")

            # Send event to Datadog
            await self._send_datadog_event(
                title=f"Data Asset {'Created' if is_new else 'Updated'}: {metadata.name}",
                text=f"Asset {metadata.asset_id} of type {metadata.asset_type.value} was {'created' if is_new else 'updated'}",
                tags=list(metadata.datadog_tags.items()),
                alert_type="info"
            )

            logger.info(f"Asset {metadata.asset_id} registered successfully", extra={
                "asset_id": metadata.asset_id,
                "asset_type": metadata.asset_type.value,
                "is_new": is_new
            })

            return True

        except Exception as e:
            logger.error(f"Failed to register asset {metadata.asset_id}: {e}")
            await self._send_error_metric("asset_registration_failed", metadata.asset_type.value)
            return False

    async def get_asset(self, asset_id: str) -> DataAssetMetadata | None:
        """Get asset metadata by ID."""
        asset = self.catalog.get(asset_id)

        if asset:
            # Record access metrics
            self.statsd.increment(
                "metadata_catalog.asset_accessed",
                tags=[
                    f"asset_type:{asset.asset_type.value}",
                    f"environment:{self.environment}"
                ]
            )

        return asset

    async def search_assets(
        self,
        query: str = "",
        asset_types: list[DataAssetType] = None,
        teams: list[str] = None,
        quality_levels: list[DataQualityLevel] = None,
        limit: int = 100
    ) -> list[DataAssetMetadata]:
        """Search assets in the catalog."""
        results = []

        for asset in self.catalog.values():
            # Apply filters
            if asset_types and asset.asset_type not in asset_types:
                continue
            if teams and asset.team not in teams:
                continue
            if quality_levels and asset.quality_level not in quality_levels:
                continue

            # Apply text search
            if query:
                searchable_text = f"{asset.name} {asset.description} {asset.owner}".lower()
                if query.lower() not in searchable_text:
                    continue

            results.append(asset)

            if len(results) >= limit:
                break

        # Record search metrics
        self.statsd.increment(
            "metadata_catalog.search_performed",
            tags=[
                f"query_length:{len(query)}",
                f"results_count:{len(results)}",
                f"environment:{self.environment}"
            ]
        )

        return results

    async def update_asset_quality(
        self,
        asset_id: str,
        quality_level: DataQualityLevel,
        quality_score: float,
        quality_details: dict[str, Any] = None
    ) -> bool:
        """Update asset quality information."""
        asset = self.catalog.get(asset_id)
        if not asset:
            logger.warning(f"Asset {asset_id} not found for quality update")
            return False

        try:
            old_quality = asset.quality_level
            old_score = asset.quality_score

            # Update quality metadata
            asset.quality_level = quality_level
            asset.quality_score = quality_score
            asset.last_quality_check = datetime.utcnow()
            asset.updated_at = datetime.utcnow()

            if quality_details:
                asset.custom_properties["quality_details"] = quality_details

            # Update Datadog tags
            asset.datadog_tags["quality_level"] = quality_level.value

            # Record change event
            await self._record_change_event(
                asset_id,
                MetadataChangeType.QUALITY_CHANGED,
                changed_fields=["quality_level", "quality_score", "last_quality_check"],
                old_values={"quality_level": old_quality.value, "quality_score": old_score},
                new_values={"quality_level": quality_level.value, "quality_score": quality_score},
                changed_by="quality_monitor"
            )

            # Send quality metrics to Datadog
            self.statsd.gauge(
                "metadata_catalog.asset_quality_score",
                quality_score,
                tags=[
                    f"asset_id:{asset_id}",
                    f"asset_type:{asset.asset_type.value}",
                    f"quality_level:{quality_level.value}",
                    f"team:{asset.team}",
                    f"environment:{self.environment}"
                ]
            )

            # Send alert if quality degraded significantly
            if old_score - quality_score > 0.2:
                await self._send_datadog_event(
                    title=f"Data Quality Alert: {asset.name}",
                    text=f"Quality score dropped from {old_score:.2f} to {quality_score:.2f}",
                    tags=list(asset.datadog_tags.items()),
                    alert_type="warning"
                )

            logger.info(f"Asset {asset_id} quality updated to {quality_level.value} ({quality_score:.2f})")
            return True

        except Exception as e:
            logger.error(f"Failed to update quality for asset {asset_id}: {e}")
            return False

    async def track_lineage(
        self,
        downstream_asset_id: str,
        upstream_asset_ids: list[str],
        operation: str = "transform"
    ) -> bool:
        """Track data lineage between assets."""
        try:
            downstream_asset = self.catalog.get(downstream_asset_id)
            if not downstream_asset:
                logger.warning(f"Downstream asset {downstream_asset_id} not found")
                return False

            # Update lineage information
            for upstream_id in upstream_asset_ids:
                upstream_asset = self.catalog.get(upstream_id)
                if upstream_asset:
                    # Update upstream dependencies
                    downstream_asset.upstream_dependencies.add(upstream_id)
                    upstream_asset.downstream_dependencies.add(downstream_asset_id)
                else:
                    logger.warning(f"Upstream asset {upstream_id} not found")

            downstream_asset.updated_at = datetime.utcnow()

            # Send lineage metrics to Datadog
            self.statsd.gauge(
                "metadata_catalog.lineage_depth",
                len(downstream_asset.upstream_dependencies),
                tags=[
                    f"asset_id:{downstream_asset_id}",
                    f"asset_type:{downstream_asset.asset_type.value}",
                    f"operation:{operation}",
                    f"environment:{self.environment}"
                ]
            )

            logger.info(f"Lineage tracked: {upstream_asset_ids} -> {downstream_asset_id}")
            return True

        except Exception as e:
            logger.error(f"Failed to track lineage: {e}")
            return False

    async def get_lineage_graph(self, asset_id: str, depth: int = 3) -> dict[str, Any]:
        """Get lineage graph for an asset."""
        def build_graph(current_id: str, current_depth: int, visited: set[str]) -> dict[str, Any]:
            if current_depth <= 0 or current_id in visited:
                return {"id": current_id, "children": []}

            visited.add(current_id)
            asset = self.catalog.get(current_id)

            if not asset:
                return {"id": current_id, "children": []}

            children = []
            for upstream_id in asset.upstream_dependencies:
                child_graph = build_graph(upstream_id, current_depth - 1, visited.copy())
                children.append(child_graph)

            return {
                "id": current_id,
                "name": asset.name,
                "type": asset.asset_type.value,
                "children": children,
                "metadata": {
                    "owner": asset.owner,
                    "team": asset.team,
                    "quality_score": asset.quality_score
                }
            }

        lineage_graph = build_graph(asset_id, depth, set())

        # Record lineage query metrics
        self.statsd.increment(
            "metadata_catalog.lineage_queried",
            tags=[
                f"asset_id:{asset_id}",
                f"depth:{depth}",
                f"environment:{self.environment}"
            ]
        )

        return lineage_graph

    async def get_catalog_health(self) -> dict[str, Any]:
        """Get catalog health metrics."""
        total_assets = len(self.catalog)

        # Calculate quality distribution
        quality_distribution = {}
        for level in DataQualityLevel:
            quality_distribution[level.value] = sum(
                1 for asset in self.catalog.values()
                if asset.quality_level == level
            )

        # Calculate type distribution
        type_distribution = {}
        for asset_type in DataAssetType:
            type_distribution[asset_type.value] = sum(
                1 for asset in self.catalog.values()
                if asset.asset_type == asset_type
            )

        # Calculate recent changes
        recent_changes = len([
            change for change in self.change_history
            if change.timestamp > datetime.utcnow() - timedelta(hours=24)
        ])

        health_metrics = {
            "total_assets": total_assets,
            "quality_distribution": quality_distribution,
            "type_distribution": type_distribution,
            "recent_changes_24h": recent_changes,
            "avg_quality_score": sum(asset.quality_score for asset in self.catalog.values()) / max(total_assets, 1),
            "last_health_check": self.last_health_check.isoformat(),
            "catalog_age_hours": (datetime.utcnow() - self.last_health_check).total_seconds() / 3600
        }

        # Send health metrics to Datadog
        for metric_name, value in health_metrics.items():
            if isinstance(value, int | float):
                self.statsd.gauge(
                    f"metadata_catalog.health.{metric_name}",
                    value,
                    tags=[f"environment:{self.environment}"]
                )

        self.last_health_check = datetime.utcnow()
        return health_metrics

    async def export_catalog(self, format: str = "json") -> str:
        """Export catalog metadata."""
        if format == "json":
            catalog_data = {
                asset_id: {
                    **asset.__dict__,
                    "created_at": asset.created_at.isoformat(),
                    "updated_at": asset.updated_at.isoformat(),
                    "last_quality_check": asset.last_quality_check.isoformat() if asset.last_quality_check else None,
                    "upstream_dependencies": list(asset.upstream_dependencies),
                    "downstream_dependencies": list(asset.downstream_dependencies),
                    "compliance_tags": list(asset.compliance_tags),
                    "pii_fields": list(asset.pii_fields)
                }
                for asset_id, asset in self.catalog.items()
            }

            return json.dumps(catalog_data, indent=2, default=str)

        raise ValueError(f"Unsupported export format: {format}")

    def _generate_asset_id(self, metadata: DataAssetMetadata) -> str:
        """Generate a unique asset ID."""
        content = f"{metadata.name}:{metadata.asset_type.value}:{metadata.location}"
        return hashlib.sha256(content.encode()).hexdigest()[:16]

    async def _record_change_event(
        self,
        asset_id: str,
        change_type: MetadataChangeType,
        changed_fields: list[str],
        old_values: dict[str, Any] = None,
        new_values: dict[str, Any] = None,
        changed_by: str = "unknown",
        change_reason: str = ""
    ):
        """Record a metadata change event."""
        event = MetadataChangeEvent(
            asset_id=asset_id,
            change_type=change_type,
            changed_fields=changed_fields,
            old_values=old_values or {},
            new_values=new_values or {},
            changed_by=changed_by,
            change_reason=change_reason
        )

        self.change_history.append(event)

        # Send change event to Datadog logs
        if self.datadog_enabled:
            {
                "timestamp": event.timestamp.isoformat(),
                "level": "INFO",
                "message": f"Metadata change: {change_type.value} for asset {asset_id}",
                "metadata_catalog": {
                    "asset_id": asset_id,
                    "change_type": change_type.value,
                    "changed_fields": changed_fields,
                    "changed_by": changed_by,
                    "environment": self.environment
                }
            }

            logger.info(f"Metadata change recorded: {change_type.value} for {asset_id}")

    async def _send_asset_metrics(self, asset: DataAssetMetadata, operation: str):
        """Send asset-related metrics to Datadog."""
        tags = [
            f"asset_type:{asset.asset_type.value}",
            f"team:{asset.team}",
            f"owner:{asset.owner}",
            f"quality_level:{asset.quality_level.value}",
            f"operation:{operation}",
            f"environment:{self.environment}"
        ]

        # Count metric
        self.statsd.increment("metadata_catalog.asset_operation", tags=tags)

        # Size metrics
        if asset.size_bytes:
            self.statsd.gauge("metadata_catalog.asset_size_bytes", asset.size_bytes, tags=tags)

        if asset.row_count:
            self.statsd.gauge("metadata_catalog.asset_row_count", asset.row_count, tags=tags)

        # Quality score
        self.statsd.gauge("metadata_catalog.asset_quality_score", asset.quality_score, tags=tags)

    async def _send_datadog_event(
        self,
        title: str,
        text: str,
        tags: list[tuple] = None,
        alert_type: str = "info"
    ):
        """Send event to Datadog."""
        if not self.datadog_enabled:
            logger.info(f"Mock Datadog event: {title} - {text}")
            return

        try:
            tag_strings = [f"{k}:{v}" for k, v in (tags or [])]
            tag_strings.append(f"environment:{self.environment}")

            # Send event using Datadog API
            if DATADOG_AVAILABLE:
                Event.create(
                    title=title,
                    text=text,
                    tags=tag_strings,
                    alert_type=alert_type,
                    source_type_name="metadata_catalog"
                )

            logger.info(f"Datadog event sent: {title}")

        except Exception as e:
            logger.warning(f"Failed to send Datadog event: {e}")

    async def _send_error_metric(self, error_type: str, asset_type: str = "unknown"):
        """Send error metrics to Datadog."""
        self.statsd.increment(
            "metadata_catalog.error",
            tags=[
                f"error_type:{error_type}",
                f"asset_type:{asset_type}",
                f"environment:{self.environment}"
            ]
        )


# Factory function for creating catalog instances
def create_metadata_catalog(
    datadog_api_key: str = None,
    datadog_app_key: str = None,
    environment: str = "development"
) -> DatadogMetadataCatalog:
    """Create a metadata catalog instance with Datadog integration."""
    return DatadogMetadataCatalog(
        datadog_api_key=datadog_api_key,
        datadog_app_key=datadog_app_key,
        environment=environment
    )


# Global catalog instance
_catalog_instance: DatadogMetadataCatalog | None = None


def get_metadata_catalog() -> DatadogMetadataCatalog:
    """Get the global metadata catalog instance."""
    global _catalog_instance

    if _catalog_instance is None:
        _catalog_instance = create_metadata_catalog()

    return _catalog_instance
