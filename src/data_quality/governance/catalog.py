"""
Data Catalog and Metadata Management
Comprehensive data catalog with automated metadata discovery and management.
"""
from __future__ import annotations

import json
import threading
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any

from core.config import get_settings
from core.logging import get_logger
from monitoring.advanced_metrics import get_metrics_collector


class AssetType(Enum):
    """Types of data assets"""
    TABLE = "table"
    VIEW = "view"
    DATASET = "dataset"
    SCHEMA = "schema"
    DATABASE = "database"
    FILE = "file"
    API = "api"
    STREAM = "stream"
    MODEL = "model"
    PIPELINE = "pipeline"


class DataClassification(Enum):
    """Data classification levels"""
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"
    HIGHLY_CONFIDENTIAL = "highly_confidential"


class DataSensitivity(Enum):
    """Data sensitivity levels"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class BusinessGlossaryTerm:
    """Business glossary term definition"""
    term_id: str
    name: str
    definition: str
    business_owner: str | None = None
    domain: str | None = None
    synonyms: list[str] = field(default_factory=list)
    related_terms: list[str] = field(default_factory=list)
    examples: list[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    version: int = 1
    status: str = "active"
    tags: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass 
class DataAssetMetadata:
    """Comprehensive metadata for data assets"""
    asset_id: str
    name: str
    asset_type: AssetType
    description: str | None = None
    
    # Business information
    business_owner: str | None = None
    technical_owner: str | None = None
    steward: str | None = None
    business_domain: str | None = None
    business_purpose: str | None = None
    
    # Technical information
    source_system: str | None = None
    location: str | None = None
    format: str | None = None
    schema_definition: dict[str, Any] = field(default_factory=dict)
    
    # Classification and sensitivity
    classification: DataClassification = DataClassification.INTERNAL
    sensitivity: DataSensitivity = DataSensitivity.LOW
    retention_period: int | None = None  # Days
    
    # Quality and usage
    quality_score: float = 0.0
    usage_count: int = 0
    last_accessed: datetime | None = None
    last_modified: datetime | None = None
    
    # Lineage and relationships
    upstream_assets: list[str] = field(default_factory=list)
    downstream_assets: list[str] = field(default_factory=list)
    related_assets: list[str] = field(default_factory=list)
    
    # Business context
    glossary_terms: list[str] = field(default_factory=list)
    business_rules: list[str] = field(default_factory=list)
    compliance_requirements: list[str] = field(default_factory=list)
    
    # Metadata management
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    version: int = 1
    status: str = "active"
    tags: list[str] = field(default_factory=list)
    
    # Custom attributes
    custom_attributes: dict[str, Any] = field(default_factory=dict)


@dataclass
class SchemaEvolution:
    """Schema evolution tracking"""
    evolution_id: str
    asset_id: str
    old_schema: dict[str, Any]
    new_schema: dict[str, Any]
    changes: list[dict[str, Any]] = field(default_factory=list)
    change_type: str = "unknown"  # added, removed, modified
    impact_assessment: dict[str, Any] = field(default_factory=dict)
    detected_at: datetime = field(default_factory=datetime.now)
    approved: bool = False
    approved_by: str | None = None
    approved_at: datetime | None = None


@dataclass
class DataGovernancePolicy:
    """Data governance policy definition"""
    policy_id: str
    name: str
    description: str
    policy_type: str  # retention, access, quality, etc.
    rules: list[dict[str, Any]] = field(default_factory=list)
    applicable_assets: list[str] = field(default_factory=list)
    enforcement_level: str = "warning"  # warning, blocking, automatic
    created_by: str | None = None
    created_at: datetime = field(default_factory=datetime.now)
    active: bool = True


class DataCatalog:
    """
    Comprehensive Data Catalog and Metadata Management System
    
    Provides:
    - Automated metadata discovery and management
    - Business glossary management
    - Data classification and tagging
    - Schema evolution tracking
    - Data governance policy enforcement
    - Search and discovery capabilities
    """

    def __init__(self, catalog_path: Path | None = None):
        self.settings = get_settings()
        self.logger = get_logger(self.__class__.__name__)
        self.metrics_collector = get_metrics_collector()
        
        # Storage paths
        self.catalog_path = catalog_path or Path("./data_catalog")
        self.assets_path = self.catalog_path / "assets"
        self.glossary_path = self.catalog_path / "glossary"
        self.policies_path = self.catalog_path / "policies"
        self.evolution_path = self.catalog_path / "evolution"
        
        self._ensure_directories()
        
        # In-memory caches
        self.assets_cache: dict[str, DataAssetMetadata] = {}
        self.glossary_cache: dict[str, BusinessGlossaryTerm] = {}
        self.policies_cache: dict[str, DataGovernancePolicy] = {}
        
        # Thread safety
        self._lock = threading.Lock()
        
        # Load existing data
        self._load_catalog_data()
        
        # Auto-discovery configuration
        self.auto_discovery_enabled = True
        self.discovery_patterns = {
            'database_tables': r'.*\.tables?\..*',
            'data_files': r'.*\.(csv|parquet|json|xlsx)$',
            'api_endpoints': r'.*\/api\/.*',
            'data_streams': r'.*\.(kafka|pubsub|kinesis)\..*'
        }

    def _ensure_directories(self):
        """Ensure catalog directories exist"""
        for path in [self.catalog_path, self.assets_path, self.glossary_path, 
                    self.policies_path, self.evolution_path]:
            path.mkdir(parents=True, exist_ok=True)

    def _load_catalog_data(self):
        """Load existing catalog data from storage"""
        try:
            # Load assets
            for asset_file in self.assets_path.glob("*.json"):
                try:
                    with open(asset_file) as f:
                        asset_data = json.load(f)
                        asset = self._dict_to_asset(asset_data)
                        self.assets_cache[asset.asset_id] = asset
                except Exception as e:
                    self.logger.error(f"Failed to load asset {asset_file}: {e}")
            
            # Load glossary terms
            for glossary_file in self.glossary_path.glob("*.json"):
                try:
                    with open(glossary_file) as f:
                        term_data = json.load(f)
                        term = self._dict_to_glossary_term(term_data)
                        self.glossary_cache[term.term_id] = term
                except Exception as e:
                    self.logger.error(f"Failed to load glossary term {glossary_file}: {e}")
            
            # Load policies
            for policy_file in self.policies_path.glob("*.json"):
                try:
                    with open(policy_file) as f:
                        policy_data = json.load(f)
                        policy = self._dict_to_policy(policy_data)
                        self.policies_cache[policy.policy_id] = policy
                except Exception as e:
                    self.logger.error(f"Failed to load policy {policy_file}: {e}")
            
            self.logger.info(
                f"Loaded catalog data: {len(self.assets_cache)} assets, "
                f"{len(self.glossary_cache)} glossary terms, "
                f"{len(self.policies_cache)} policies"
            )
            
        except Exception as e:
            self.logger.error(f"Failed to load catalog data: {e}")

    def register_asset(
        self,
        asset_metadata: DataAssetMetadata,
        auto_discover: bool = True
    ) -> str:
        """
        Register a new data asset in the catalog
        
        Args:
            asset_metadata: Asset metadata
            auto_discover: Whether to auto-discover additional metadata
        
        Returns:
            Asset ID
        """
        try:
            with self._lock:
                # Auto-discovery if enabled
                if auto_discover and self.auto_discovery_enabled:
                    asset_metadata = self._enhance_asset_metadata(asset_metadata)
                
                # Set timestamps
                asset_metadata.created_at = datetime.now()
                asset_metadata.updated_at = datetime.now()
                
                # Store in cache and persist
                self.assets_cache[asset_metadata.asset_id] = asset_metadata
                self._persist_asset(asset_metadata)
                
                # Record metrics
                self._record_catalog_metrics("asset_registered")
                
                self.logger.info(f"Registered asset: {asset_metadata.asset_id}")
                return asset_metadata.asset_id
                
        except Exception as e:
            self.logger.error(f"Failed to register asset {asset_metadata.asset_id}: {e}")
            raise

    def update_asset(
        self,
        asset_id: str,
        updates: dict[str, Any],
        track_changes: bool = True
    ) -> bool:
        """
        Update asset metadata
        
        Args:
            asset_id: Asset identifier
            updates: Dictionary of field updates
            track_changes: Whether to track schema changes
        
        Returns:
            Success status
        """
        try:
            with self._lock:
                if asset_id not in self.assets_cache:
                    raise ValueError(f"Asset {asset_id} not found")
                
                asset = self.assets_cache[asset_id]
                
                # Track schema changes
                if track_changes and 'schema_definition' in updates:
                    old_schema = asset.schema_definition.copy()
                    new_schema = updates['schema_definition']
                    
                    if old_schema != new_schema:
                        self._track_schema_evolution(asset_id, old_schema, new_schema)
                
                # Apply updates
                for field, value in updates.items():
                    if hasattr(asset, field):
                        setattr(asset, field, value)
                
                # Update version and timestamp
                asset.version += 1
                asset.updated_at = datetime.now()
                
                # Persist changes
                self._persist_asset(asset)
                
                self.logger.info(f"Updated asset: {asset_id}")
                return True
                
        except Exception as e:
            self.logger.error(f"Failed to update asset {asset_id}: {e}")
            return False

    def get_asset(self, asset_id: str) -> DataAssetMetadata | None:
        """Get asset metadata by ID"""
        with self._lock:
            return self.assets_cache.get(asset_id)

    def search_assets(
        self,
        query: str | None = None,
        asset_type: AssetType | None = None,
        classification: DataClassification | None = None,
        business_domain: str | None = None,
        tags: list[str] | None = None,
        limit: int = 100
    ) -> list[DataAssetMetadata]:
        """
        Search assets with filters
        
        Args:
            query: Text search query
            asset_type: Filter by asset type
            classification: Filter by classification
            business_domain: Filter by domain
            tags: Filter by tags
            limit: Maximum results
        
        Returns:
            List of matching assets
        """
        try:
            with self._lock:
                results = []
                
                for asset in self.assets_cache.values():
                    # Apply filters
                    if asset_type and asset.asset_type != asset_type:
                        continue
                    if classification and asset.classification != classification:
                        continue
                    if business_domain and asset.business_domain != business_domain:
                        continue
                    if tags and not any(tag in asset.tags for tag in tags):
                        continue
                    
                    # Text search
                    if query:
                        search_text = f"{asset.name} {asset.description} {' '.join(asset.tags)}".lower()
                        if query.lower() not in search_text:
                            continue
                    
                    results.append(asset)
                    
                    if len(results) >= limit:
                        break
                
                # Sort by relevance (simplified)
                if query:
                    results.sort(key=lambda a: query.lower() in a.name.lower(), reverse=True)
                
                return results
                
        except Exception as e:
            self.logger.error(f"Asset search failed: {e}")
            return []

    def add_glossary_term(self, term: BusinessGlossaryTerm) -> str:
        """Add business glossary term"""
        try:
            with self._lock:
                term.created_at = datetime.now()
                term.updated_at = datetime.now()
                
                self.glossary_cache[term.term_id] = term
                self._persist_glossary_term(term)
                
                self.logger.info(f"Added glossary term: {term.term_id}")
                return term.term_id
                
        except Exception as e:
            self.logger.error(f"Failed to add glossary term {term.term_id}: {e}")
            raise

    def update_glossary_term(
        self, 
        term_id: str, 
        updates: dict[str, Any]
    ) -> bool:
        """Update glossary term"""
        try:
            with self._lock:
                if term_id not in self.glossary_cache:
                    raise ValueError(f"Glossary term {term_id} not found")
                
                term = self.glossary_cache[term_id]
                
                # Apply updates
                for field, value in updates.items():
                    if hasattr(term, field):
                        setattr(term, field, value)
                
                term.version += 1
                term.updated_at = datetime.now()
                
                self._persist_glossary_term(term)
                
                self.logger.info(f"Updated glossary term: {term_id}")
                return True
                
        except Exception as e:
            self.logger.error(f"Failed to update glossary term {term_id}: {e}")
            return False

    def search_glossary(
        self,
        query: str | None = None,
        domain: str | None = None,
        limit: int = 50
    ) -> list[BusinessGlossaryTerm]:
        """Search business glossary"""
        try:
            with self._lock:
                results = []
                
                for term in self.glossary_cache.values():
                    if term.status != "active":
                        continue
                    
                    if domain and term.domain != domain:
                        continue
                    
                    if query:
                        search_text = f"{term.name} {term.definition} {' '.join(term.tags)}".lower()
                        if query.lower() not in search_text:
                            continue
                    
                    results.append(term)
                    
                    if len(results) >= limit:
                        break
                
                return results
                
        except Exception as e:
            self.logger.error(f"Glossary search failed: {e}")
            return []

    def link_asset_to_terms(self, asset_id: str, term_ids: list[str]) -> bool:
        """Link asset to business glossary terms"""
        try:
            with self._lock:
                if asset_id not in self.assets_cache:
                    raise ValueError(f"Asset {asset_id} not found")
                
                asset = self.assets_cache[asset_id]
                
                # Validate term IDs
                valid_terms = [
                    term_id for term_id in term_ids 
                    if term_id in self.glossary_cache
                ]
                
                if len(valid_terms) != len(term_ids):
                    invalid_terms = set(term_ids) - set(valid_terms)
                    self.logger.warning(f"Invalid term IDs: {invalid_terms}")
                
                # Update asset
                asset.glossary_terms = list(set(asset.glossary_terms + valid_terms))
                asset.updated_at = datetime.now()
                
                self._persist_asset(asset)
                
                self.logger.info(f"Linked asset {asset_id} to {len(valid_terms)} terms")
                return True
                
        except Exception as e:
            self.logger.error(f"Failed to link asset {asset_id} to terms: {e}")
            return False

    def add_governance_policy(self, policy: DataGovernancePolicy) -> str:
        """Add data governance policy"""
        try:
            with self._lock:
                policy.created_at = datetime.now()
                
                self.policies_cache[policy.policy_id] = policy
                self._persist_policy(policy)
                
                self.logger.info(f"Added governance policy: {policy.policy_id}")
                return policy.policy_id
                
        except Exception as e:
            self.logger.error(f"Failed to add policy {policy.policy_id}: {e}")
            raise

    def evaluate_policies(self, asset_id: str) -> list[dict[str, Any]]:
        """Evaluate governance policies against an asset"""
        try:
            with self._lock:
                if asset_id not in self.assets_cache:
                    raise ValueError(f"Asset {asset_id} not found")
                
                asset = self.assets_cache[asset_id]
                violations = []
                
                for policy in self.policies_cache.values():
                    if not policy.active:
                        continue
                    
                    # Check if policy applies to this asset
                    if (policy.applicable_assets and 
                        asset_id not in policy.applicable_assets):
                        continue
                    
                    # Evaluate policy rules
                    for rule in policy.rules:
                        violation = self._evaluate_policy_rule(asset, rule, policy)
                        if violation:
                            violations.append(violation)
                
                return violations
                
        except Exception as e:
            self.logger.error(f"Policy evaluation failed for {asset_id}: {e}")
            return []

    def get_asset_lineage(
        self, 
        asset_id: str, 
        direction: str = "both",
        max_depth: int = 3
    ) -> dict[str, Any]:
        """
        Get asset lineage information
        
        Args:
            asset_id: Asset identifier
            direction: "upstream", "downstream", or "both"
            max_depth: Maximum traversal depth
        
        Returns:
            Lineage graph
        """
        try:
            with self._lock:
                if asset_id not in self.assets_cache:
                    raise ValueError(f"Asset {asset_id} not found")
                
                lineage = {
                    "asset_id": asset_id,
                    "upstream": [],
                    "downstream": [],
                    "metadata": {}
                }
                
                if direction in ["upstream", "both"]:
                    lineage["upstream"] = self._traverse_lineage(
                        asset_id, "upstream", max_depth
                    )
                
                if direction in ["downstream", "both"]:
                    lineage["downstream"] = self._traverse_lineage(
                        asset_id, "downstream", max_depth
                    )
                
                return lineage
                
        except Exception as e:
            self.logger.error(f"Lineage retrieval failed for {asset_id}: {e}")
            return {}

    def get_catalog_statistics(self) -> dict[str, Any]:
        """Get catalog statistics"""
        try:
            with self._lock:
                stats = {
                    "total_assets": len(self.assets_cache),
                    "asset_types": {},
                    "classifications": {},
                    "business_domains": {},
                    "total_glossary_terms": len(self.glossary_cache),
                    "total_policies": len(self.policies_cache),
                    "quality_scores": {
                        "high": 0,  # > 0.8
                        "medium": 0,  # 0.5 - 0.8
                        "low": 0     # < 0.5
                    }
                }
                
                # Analyze assets
                for asset in self.assets_cache.values():
                    # Asset types
                    asset_type = asset.asset_type.value
                    stats["asset_types"][asset_type] = stats["asset_types"].get(asset_type, 0) + 1
                    
                    # Classifications
                    classification = asset.classification.value
                    stats["classifications"][classification] = stats["classifications"].get(classification, 0) + 1
                    
                    # Business domains
                    if asset.business_domain:
                        domain = asset.business_domain
                        stats["business_domains"][domain] = stats["business_domains"].get(domain, 0) + 1
                    
                    # Quality scores
                    if asset.quality_score > 0.8:
                        stats["quality_scores"]["high"] += 1
                    elif asset.quality_score >= 0.5:
                        stats["quality_scores"]["medium"] += 1
                    else:
                        stats["quality_scores"]["low"] += 1
                
                return stats
                
        except Exception as e:
            self.logger.error(f"Statistics calculation failed: {e}")
            return {}

    def _enhance_asset_metadata(self, asset: DataAssetMetadata) -> DataAssetMetadata:
        """Auto-discover and enhance asset metadata"""
        try:
            # Schema inference based on location/format
            if asset.location and not asset.schema_definition:
                schema = self._infer_schema(asset.location, asset.format)
                if schema:
                    asset.schema_definition = schema
            
            # Auto-classification based on patterns
            if asset.classification == DataClassification.INTERNAL:
                inferred_classification = self._infer_classification(asset)
                if inferred_classification:
                    asset.classification = inferred_classification
            
            # Auto-tagging based on name and schema
            auto_tags = self._generate_auto_tags(asset)
            asset.tags.extend([tag for tag in auto_tags if tag not in asset.tags])
            
            return asset
            
        except Exception as e:
            self.logger.error(f"Asset metadata enhancement failed: {e}")
            return asset

    def _infer_schema(self, location: str, format: str | None) -> dict[str, Any] | None:
        """Infer schema from asset location and format"""
        # This is a placeholder - in practice, you'd implement
        # actual schema inference based on the data source
        return None

    def _infer_classification(self, asset: DataAssetMetadata) -> DataClassification | None:
        """Infer data classification based on asset characteristics"""
        # Simple heuristics - in practice, use more sophisticated rules
        name_lower = asset.name.lower()
        
        sensitive_keywords = ['ssn', 'social', 'credit', 'password', 'personal']
        if any(keyword in name_lower for keyword in sensitive_keywords):
            return DataClassification.CONFIDENTIAL
        
        internal_keywords = ['employee', 'internal', 'hr']
        if any(keyword in name_lower for keyword in internal_keywords):
            return DataClassification.INTERNAL
        
        return None

    def _generate_auto_tags(self, asset: DataAssetMetadata) -> list[str]:
        """Generate automatic tags based on asset characteristics"""
        tags = []
        
        # Tags based on asset type
        tags.append(asset.asset_type.value)
        
        # Tags based on format
        if asset.format:
            tags.append(f"format_{asset.format}")
        
        # Tags based on business domain
        if asset.business_domain:
            tags.append(f"domain_{asset.business_domain.lower().replace(' ', '_')}")
        
        # Tags based on name patterns
        name_lower = asset.name.lower()
        if 'fact' in name_lower:
            tags.append('fact_table')
        elif 'dim' in name_lower:
            tags.append('dimension_table')
        
        return tags

    def _track_schema_evolution(
        self,
        asset_id: str,
        old_schema: dict[str, Any],
        new_schema: dict[str, Any]
    ):
        """Track schema evolution changes"""
        try:
            evolution = SchemaEvolution(
                evolution_id=f"{asset_id}_{int(datetime.now().timestamp())}",
                asset_id=asset_id,
                old_schema=old_schema,
                new_schema=new_schema,
                changes=self._compare_schemas(old_schema, new_schema)
            )
            
            evolution_file = self.evolution_path / f"{evolution.evolution_id}.json"
            with open(evolution_file, 'w') as f:
                json.dump(self._evolution_to_dict(evolution), f, indent=2, default=str)
            
            self.logger.info(f"Tracked schema evolution for asset {asset_id}")
            
        except Exception as e:
            self.logger.error(f"Schema evolution tracking failed: {e}")

    def _compare_schemas(
        self, 
        old_schema: dict[str, Any], 
        new_schema: dict[str, Any]
    ) -> list[dict[str, Any]]:
        """Compare schemas and identify changes"""
        changes = []
        
        try:
            old_fields = set(old_schema.get('fields', {}).keys())
            new_fields = set(new_schema.get('fields', {}).keys())
            
            # Added fields
            for field in new_fields - old_fields:
                changes.append({
                    'type': 'field_added',
                    'field': field,
                    'new_definition': new_schema['fields'][field]
                })
            
            # Removed fields
            for field in old_fields - new_fields:
                changes.append({
                    'type': 'field_removed',
                    'field': field,
                    'old_definition': old_schema['fields'][field]
                })
            
            # Modified fields
            common_fields = old_fields & new_fields
            for field in common_fields:
                old_def = old_schema['fields'][field]
                new_def = new_schema['fields'][field]
                
                if old_def != new_def:
                    changes.append({
                        'type': 'field_modified',
                        'field': field,
                        'old_definition': old_def,
                        'new_definition': new_def
                    })
            
        except Exception as e:
            self.logger.error(f"Schema comparison failed: {e}")
        
        return changes

    def _evaluate_policy_rule(
        self, 
        asset: DataAssetMetadata, 
        rule: dict[str, Any],
        policy: DataGovernancePolicy
    ) -> dict[str, Any] | None:
        """Evaluate a single policy rule against an asset"""
        try:
            rule_type = rule.get('type')
            
            if rule_type == 'retention_check':
                # Check if asset has appropriate retention period
                max_retention = rule.get('max_retention_days', 365)
                if asset.retention_period and asset.retention_period > max_retention:
                    return {
                        'policy_id': policy.policy_id,
                        'rule': rule,
                        'violation_type': 'retention_exceeded',
                        'details': f"Retention period {asset.retention_period} exceeds maximum {max_retention}",
                        'severity': rule.get('severity', 'medium')
                    }
            
            elif rule_type == 'classification_check':
                # Check if asset has appropriate classification
                required_classification = rule.get('minimum_classification')
                if required_classification:
                    classification_levels = {
                        'public': 1,
                        'internal': 2,
                        'confidential': 3,
                        'restricted': 4,
                        'highly_confidential': 5
                    }
                    
                    asset_level = classification_levels.get(asset.classification.value, 0)
                    required_level = classification_levels.get(required_classification, 0)
                    
                    if asset_level < required_level:
                        return {
                            'policy_id': policy.policy_id,
                            'rule': rule,
                            'violation_type': 'insufficient_classification',
                            'details': f"Asset classification {asset.classification.value} below required {required_classification}",
                            'severity': rule.get('severity', 'high')
                        }
            
            elif rule_type == 'ownership_check':
                # Check if asset has required ownership information
                if rule.get('require_business_owner', False) and not asset.business_owner:
                    return {
                        'policy_id': policy.policy_id,
                        'rule': rule,
                        'violation_type': 'missing_business_owner',
                        'details': "Asset missing required business owner",
                        'severity': rule.get('severity', 'medium')
                    }
            
            return None
            
        except Exception as e:
            self.logger.error(f"Policy rule evaluation failed: {e}")
            return None

    def _traverse_lineage(
        self, 
        asset_id: str, 
        direction: str, 
        max_depth: int,
        visited: set | None = None
    ) -> list[dict[str, Any]]:
        """Recursively traverse asset lineage"""
        if visited is None:
            visited = set()
        
        if asset_id in visited or max_depth <= 0:
            return []
        
        visited.add(asset_id)
        lineage = []
        
        try:
            asset = self.assets_cache.get(asset_id)
            if not asset:
                return lineage
            
            # Get connected assets based on direction
            if direction == "upstream":
                connected_assets = asset.upstream_assets
            else:  # downstream
                connected_assets = asset.downstream_assets
            
            for connected_id in connected_assets:
                connected_asset = self.assets_cache.get(connected_id)
                if connected_asset:
                    lineage_entry = {
                        'asset_id': connected_id,
                        'name': connected_asset.name,
                        'asset_type': connected_asset.asset_type.value,
                        'depth': max_depth,
                        'children': self._traverse_lineage(
                            connected_id, direction, max_depth - 1, visited.copy()
                        )
                    }
                    lineage.append(lineage_entry)
            
        except Exception as e:
            self.logger.error(f"Lineage traversal failed for {asset_id}: {e}")
        
        return lineage

    def _persist_asset(self, asset: DataAssetMetadata):
        """Persist asset to storage"""
        try:
            asset_file = self.assets_path / f"{asset.asset_id}.json"
            with open(asset_file, 'w') as f:
                json.dump(self._asset_to_dict(asset), f, indent=2, default=str)
        except Exception as e:
            self.logger.error(f"Failed to persist asset {asset.asset_id}: {e}")

    def _persist_glossary_term(self, term: BusinessGlossaryTerm):
        """Persist glossary term to storage"""
        try:
            term_file = self.glossary_path / f"{term.term_id}.json"
            with open(term_file, 'w') as f:
                json.dump(self._glossary_term_to_dict(term), f, indent=2, default=str)
        except Exception as e:
            self.logger.error(f"Failed to persist glossary term {term.term_id}: {e}")

    def _persist_policy(self, policy: DataGovernancePolicy):
        """Persist policy to storage"""
        try:
            policy_file = self.policies_path / f"{policy.policy_id}.json"
            with open(policy_file, 'w') as f:
                json.dump(self._policy_to_dict(policy), f, indent=2, default=str)
        except Exception as e:
            self.logger.error(f"Failed to persist policy {policy.policy_id}: {e}")

    def _record_catalog_metrics(self, operation: str):
        """Record catalog operation metrics"""
        try:
            self.metrics_collector.increment_counter(
                f"data_catalog_{operation}",
                1,
                {"catalog": "main"}
            )
        except Exception as e:
            self.logger.error(f"Failed to record catalog metrics: {e}")

    # Serialization helpers
    def _asset_to_dict(self, asset: DataAssetMetadata) -> dict[str, Any]:
        """Convert asset to dictionary"""
        return {
            'asset_id': asset.asset_id,
            'name': asset.name,
            'asset_type': asset.asset_type.value,
            'description': asset.description,
            'business_owner': asset.business_owner,
            'technical_owner': asset.technical_owner,
            'steward': asset.steward,
            'business_domain': asset.business_domain,
            'business_purpose': asset.business_purpose,
            'source_system': asset.source_system,
            'location': asset.location,
            'format': asset.format,
            'schema_definition': asset.schema_definition,
            'classification': asset.classification.value,
            'sensitivity': asset.sensitivity.value,
            'retention_period': asset.retention_period,
            'quality_score': asset.quality_score,
            'usage_count': asset.usage_count,
            'last_accessed': asset.last_accessed.isoformat() if asset.last_accessed else None,
            'last_modified': asset.last_modified.isoformat() if asset.last_modified else None,
            'upstream_assets': asset.upstream_assets,
            'downstream_assets': asset.downstream_assets,
            'related_assets': asset.related_assets,
            'glossary_terms': asset.glossary_terms,
            'business_rules': asset.business_rules,
            'compliance_requirements': asset.compliance_requirements,
            'created_at': asset.created_at.isoformat(),
            'updated_at': asset.updated_at.isoformat(),
            'version': asset.version,
            'status': asset.status,
            'tags': asset.tags,
            'custom_attributes': asset.custom_attributes
        }

    def _dict_to_asset(self, data: dict[str, Any]) -> DataAssetMetadata:
        """Convert dictionary to asset"""
        return DataAssetMetadata(
            asset_id=data['asset_id'],
            name=data['name'],
            asset_type=AssetType(data['asset_type']),
            description=data.get('description'),
            business_owner=data.get('business_owner'),
            technical_owner=data.get('technical_owner'),
            steward=data.get('steward'),
            business_domain=data.get('business_domain'),
            business_purpose=data.get('business_purpose'),
            source_system=data.get('source_system'),
            location=data.get('location'),
            format=data.get('format'),
            schema_definition=data.get('schema_definition', {}),
            classification=DataClassification(data.get('classification', 'internal')),
            sensitivity=DataSensitivity(data.get('sensitivity', 'low')),
            retention_period=data.get('retention_period'),
            quality_score=data.get('quality_score', 0.0),
            usage_count=data.get('usage_count', 0),
            last_accessed=datetime.fromisoformat(data['last_accessed']) if data.get('last_accessed') else None,
            last_modified=datetime.fromisoformat(data['last_modified']) if data.get('last_modified') else None,
            upstream_assets=data.get('upstream_assets', []),
            downstream_assets=data.get('downstream_assets', []),
            related_assets=data.get('related_assets', []),
            glossary_terms=data.get('glossary_terms', []),
            business_rules=data.get('business_rules', []),
            compliance_requirements=data.get('compliance_requirements', []),
            created_at=datetime.fromisoformat(data['created_at']),
            updated_at=datetime.fromisoformat(data['updated_at']),
            version=data.get('version', 1),
            status=data.get('status', 'active'),
            tags=data.get('tags', []),
            custom_attributes=data.get('custom_attributes', {})
        )

    def _glossary_term_to_dict(self, term: BusinessGlossaryTerm) -> dict[str, Any]:
        """Convert glossary term to dictionary"""
        return {
            'term_id': term.term_id,
            'name': term.name,
            'definition': term.definition,
            'business_owner': term.business_owner,
            'domain': term.domain,
            'synonyms': term.synonyms,
            'related_terms': term.related_terms,
            'examples': term.examples,
            'created_at': term.created_at.isoformat(),
            'updated_at': term.updated_at.isoformat(),
            'version': term.version,
            'status': term.status,
            'tags': term.tags,
            'metadata': term.metadata
        }

    def _dict_to_glossary_term(self, data: dict[str, Any]) -> BusinessGlossaryTerm:
        """Convert dictionary to glossary term"""
        return BusinessGlossaryTerm(
            term_id=data['term_id'],
            name=data['name'],
            definition=data['definition'],
            business_owner=data.get('business_owner'),
            domain=data.get('domain'),
            synonyms=data.get('synonyms', []),
            related_terms=data.get('related_terms', []),
            examples=data.get('examples', []),
            created_at=datetime.fromisoformat(data['created_at']),
            updated_at=datetime.fromisoformat(data['updated_at']),
            version=data.get('version', 1),
            status=data.get('status', 'active'),
            tags=data.get('tags', []),
            metadata=data.get('metadata', {})
        )

    def _policy_to_dict(self, policy: DataGovernancePolicy) -> dict[str, Any]:
        """Convert policy to dictionary"""
        return {
            'policy_id': policy.policy_id,
            'name': policy.name,
            'description': policy.description,
            'policy_type': policy.policy_type,
            'rules': policy.rules,
            'applicable_assets': policy.applicable_assets,
            'enforcement_level': policy.enforcement_level,
            'created_by': policy.created_by,
            'created_at': policy.created_at.isoformat(),
            'active': policy.active
        }

    def _dict_to_policy(self, data: dict[str, Any]) -> DataGovernancePolicy:
        """Convert dictionary to policy"""
        return DataGovernancePolicy(
            policy_id=data['policy_id'],
            name=data['name'],
            description=data['description'],
            policy_type=data['policy_type'],
            rules=data.get('rules', []),
            applicable_assets=data.get('applicable_assets', []),
            enforcement_level=data.get('enforcement_level', 'warning'),
            created_by=data.get('created_by'),
            created_at=datetime.fromisoformat(data['created_at']),
            active=data.get('active', True)
        )

    def _evolution_to_dict(self, evolution: SchemaEvolution) -> dict[str, Any]:
        """Convert schema evolution to dictionary"""
        return {
            'evolution_id': evolution.evolution_id,
            'asset_id': evolution.asset_id,
            'old_schema': evolution.old_schema,
            'new_schema': evolution.new_schema,
            'changes': evolution.changes,
            'change_type': evolution.change_type,
            'impact_assessment': evolution.impact_assessment,
            'detected_at': evolution.detected_at.isoformat(),
            'approved': evolution.approved,
            'approved_by': evolution.approved_by,
            'approved_at': evolution.approved_at.isoformat() if evolution.approved_at else None
        }