"""
Enterprise Data Governance and Lineage Tracking Framework
Provides comprehensive data cataloging, metadata management, data lineage tracking,
privacy impact assessments, and data lifecycle management.
"""
import hashlib
import json
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from collections import defaultdict, deque
import networkx as nx

from pydantic import BaseModel, Field

from core.logging import get_logger
from core.security.advanced_security import AuditLogger, ActionType


logger = get_logger(__name__)


class DataClassification(Enum):
    """Data classification levels for governance"""
    PUBLIC = "public"
    INTERNAL = "internal"
    CONFIDENTIAL = "confidential"
    RESTRICTED = "restricted"
    TOP_SECRET = "top_secret"


class DataFormat(Enum):
    """Data formats"""
    CSV = "csv"
    JSON = "json"
    PARQUET = "parquet"
    AVRO = "avro"
    DELTA = "delta"
    ICEBERG = "iceberg"
    XML = "xml"
    DATABASE_TABLE = "database_table"
    API_ENDPOINT = "api_endpoint"


class DataQuality(Enum):
    """Data quality levels"""
    EXCELLENT = "excellent"
    GOOD = "good"
    FAIR = "fair"
    POOR = "poor"
    UNKNOWN = "unknown"


class PrivacyImpactLevel(Enum):
    """Privacy impact assessment levels"""
    NO_IMPACT = "no_impact"
    LOW_IMPACT = "low_impact"
    MODERATE_IMPACT = "moderate_impact"
    HIGH_IMPACT = "high_impact"
    CRITICAL_IMPACT = "critical_impact"


class DataLifecycleStage(Enum):
    """Data lifecycle stages"""
    CREATED = "created"
    PROCESSED = "processed"
    STORED = "stored"
    ACCESSED = "accessed"
    SHARED = "shared"
    ARCHIVED = "archived"
    PURGED = "purged"


class LineageEventType(Enum):
    """Types of lineage events"""
    CREATE = "create"
    READ = "read"
    WRITE = "write"
    TRANSFORM = "transform"
    COPY = "copy"
    MOVE = "move"
    DELETE = "delete"
    MERGE = "merge"
    JOIN = "join"
    AGGREGATE = "aggregate"
    FILTER = "filter"


@dataclass
class DataSchema:
    """Data schema definition"""
    schema_id: str
    name: str
    version: str
    fields: List[Dict[str, Any]]
    format: DataFormat
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DataAsset:
    """Comprehensive data asset definition"""
    asset_id: str
    name: str
    description: str
    asset_type: str
    location: str
    format: DataFormat
    schema: Optional[DataSchema] = None
    
    # Classification and quality
    classification: DataClassification = DataClassification.INTERNAL
    quality_score: float = 0.0
    quality_level: DataQuality = DataQuality.UNKNOWN
    
    # Governance information
    owner: str = ""
    steward: str = ""
    business_glossary_terms: Set[str] = field(default_factory=set)
    tags: Set[str] = field(default_factory=set)
    
    # Privacy and compliance
    contains_pii: bool = False
    contains_phi: bool = False
    gdpr_applicable: bool = False
    retention_period: Optional[timedelta] = None
    privacy_impact_level: PrivacyImpactLevel = PrivacyImpactLevel.NO_IMPACT
    
    # Lifecycle
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    last_accessed: Optional[datetime] = None
    lifecycle_stage: DataLifecycleStage = DataLifecycleStage.CREATED
    
    # Technical metadata
    size_bytes: Optional[int] = None
    row_count: Optional[int] = None
    compression: Optional[str] = None
    checksum: Optional[str] = None
    
    # Relationships
    parent_assets: Set[str] = field(default_factory=set)
    child_assets: Set[str] = field(default_factory=set)
    related_assets: Set[str] = field(default_factory=set)
    
    # Additional metadata
    custom_metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class LineageEvent:
    """Data lineage event record"""
    event_id: str
    timestamp: datetime
    event_type: LineageEventType
    source_asset_id: Optional[str]
    target_asset_id: str
    operation: str
    operation_details: Dict[str, Any]
    user_id: Optional[str]
    system_id: str
    session_id: Optional[str]
    transformation_logic: Optional[str] = None
    data_quality_impact: Optional[Dict[str, Any]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DataLineage:
    """Data lineage representation"""
    lineage_id: str
    asset_id: str
    upstream_assets: Set[str] = field(default_factory=set)
    downstream_assets: Set[str] = field(default_factory=set)
    lineage_events: List[str] = field(default_factory=list)  # Event IDs
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)


@dataclass
class BusinessGlossaryTerm:
    """Business glossary term definition"""
    term_id: str
    name: str
    definition: str
    business_definition: str
    technical_definition: str
    synonyms: List[str] = field(default_factory=list)
    related_terms: Set[str] = field(default_factory=set)
    category: str = ""
    owner: str = ""
    approved: bool = False
    created_at: datetime = field(default_factory=datetime.now)
    metadata: Dict[str, Any] = field(default_factory=dict)


@dataclass
class DataRetentionPolicy:
    """Data retention policy"""
    policy_id: str
    name: str
    description: str
    data_categories: List[str]
    retention_period: timedelta
    disposal_method: str  # delete, archive, anonymize
    legal_basis: str
    exceptions: List[str] = field(default_factory=list)
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)


@dataclass
class PrivacyImpactAssessment:
    """Privacy Impact Assessment (PIA)"""
    pia_id: str
    asset_id: str
    assessment_date: datetime
    assessor: str
    
    # Privacy risk factors
    data_types_processed: List[str]
    processing_purposes: List[str]
    data_subjects: List[str]
    third_party_sharing: bool
    international_transfers: bool
    
    # Risk assessment
    privacy_risks: List[Dict[str, Any]]
    mitigation_measures: List[str]
    residual_risk_level: PrivacyImpactLevel
    
    # Compliance
    legal_basis: str
    consent_required: bool
    legitimate_interest: bool
    
    # Review
    review_date: datetime
    approved: bool = False
    approver: Optional[str] = None
    
    metadata: Dict[str, Any] = field(default_factory=dict)


class DataCatalog:
    """Comprehensive data catalog for enterprise data governance"""
    
    def __init__(self, audit_logger: Optional[AuditLogger] = None):
        self.logger = get_logger(__name__)
        self.audit_logger = audit_logger or AuditLogger()
        
        # Core catalog data
        self.assets: Dict[str, DataAsset] = {}
        self.schemas: Dict[str, DataSchema] = {}
        self.business_glossary: Dict[str, BusinessGlossaryTerm] = {}
        
        # Search indexes
        self._name_index: Dict[str, Set[str]] = defaultdict(set)
        self._tag_index: Dict[str, Set[str]] = defaultdict(set)
        self._owner_index: Dict[str, Set[str]] = defaultdict(set)
        self._classification_index: Dict[DataClassification, Set[str]] = defaultdict(set)
        
    def register_asset(self, asset: DataAsset) -> str:
        """Register a new data asset in the catalog"""
        
        # Generate ID if not provided
        if not asset.asset_id:
            asset.asset_id = str(uuid.uuid4())
        
        # Calculate checksum if data location is accessible
        if not asset.checksum:
            asset.checksum = self._calculate_checksum(asset.location)
        
        # Store asset
        self.assets[asset.asset_id] = asset
        
        # Update indexes
        self._update_indexes(asset)
        
        # Audit log
        self.audit_logger.log_audit_event(
            user_id="system",
            action=ActionType.CREATE,
            resource_type="data_asset",
            resource_id=asset.asset_id,
            new_value={
                "name": asset.name,
                "type": asset.asset_type,
                "classification": asset.classification.value,
                "location": asset.location
            }
        )
        
        self.logger.info(f"Registered data asset: {asset.name} ({asset.asset_id})")
        return asset.asset_id
    
    def update_asset(self, asset_id: str, updates: Dict[str, Any]) -> bool:
        """Update an existing data asset"""
        
        if asset_id not in self.assets:
            return False
        
        asset = self.assets[asset_id]
        old_values = {}
        
        # Apply updates
        for key, value in updates.items():
            if hasattr(asset, key):
                old_values[key] = getattr(asset, key)
                setattr(asset, key, value)
        
        asset.updated_at = datetime.now()
        
        # Update indexes
        self._update_indexes(asset)
        
        # Audit log
        self.audit_logger.log_audit_event(
            user_id="system",
            action=ActionType.UPDATE,
            resource_type="data_asset",
            resource_id=asset_id,
            old_value=old_values,
            new_value=updates
        )
        
        return True
    
    def get_asset(self, asset_id: str) -> Optional[DataAsset]:
        """Get data asset by ID"""
        return self.assets.get(asset_id)
    
    def search_assets(
        self,
        query: Optional[str] = None,
        tags: Optional[List[str]] = None,
        owner: Optional[str] = None,
        classification: Optional[DataClassification] = None,
        asset_type: Optional[str] = None,
        contains_pii: Optional[bool] = None
    ) -> List[DataAsset]:
        """Search data assets with various filters"""
        
        result_sets = []
        
        # Text search
        if query:
            query_lower = query.lower()
            matching_assets = set()
            
            for asset_id, asset in self.assets.items():
                if (query_lower in asset.name.lower() or 
                    query_lower in asset.description.lower()):
                    matching_assets.add(asset_id)
            
            result_sets.append(matching_assets)
        
        # Tag search
        if tags:
            tag_matches = set()
            for tag in tags:
                tag_matches.update(self._tag_index.get(tag, set()))
            result_sets.append(tag_matches)
        
        # Owner search
        if owner:
            result_sets.append(self._owner_index.get(owner, set()))
        
        # Classification search
        if classification:
            result_sets.append(self._classification_index.get(classification, set()))
        
        # Type search
        if asset_type:
            type_matches = {
                asset_id for asset_id, asset in self.assets.items()
                if asset.asset_type == asset_type
            }
            result_sets.append(type_matches)
        
        # PII search
        if contains_pii is not None:
            pii_matches = {
                asset_id for asset_id, asset in self.assets.items()
                if asset.contains_pii == contains_pii
            }
            result_sets.append(pii_matches)
        
        # Intersect all result sets
        if result_sets:
            final_results = set.intersection(*result_sets)
        else:
            final_results = set(self.assets.keys())
        
        return [self.assets[asset_id] for asset_id in final_results]
    
    def add_business_term(self, term: BusinessGlossaryTerm) -> str:
        """Add term to business glossary"""
        
        if not term.term_id:
            term.term_id = str(uuid.uuid4())
        
        self.business_glossary[term.term_id] = term
        
        self.logger.info(f"Added business glossary term: {term.name}")
        return term.term_id
    
    def link_asset_to_term(self, asset_id: str, term_id: str) -> bool:
        """Link data asset to business glossary term"""
        
        if asset_id not in self.assets or term_id not in self.business_glossary:
            return False
        
        asset = self.assets[asset_id]
        term = self.business_glossary[term_id]
        
        asset.business_glossary_terms.add(term_id)
        
        self.logger.info(f"Linked asset {asset.name} to term {term.name}")
        return True
    
    def _calculate_checksum(self, location: str) -> Optional[str]:
        """Calculate checksum for data asset"""
        try:
            # This would integrate with your storage systems
            # Placeholder implementation
            return hashlib.md5(location.encode()).hexdigest()
        except Exception as e:
            self.logger.warning(f"Could not calculate checksum for {location}: {e}")
            return None
    
    def _update_indexes(self, asset: DataAsset):
        """Update search indexes for asset"""
        
        # Name index (simple keyword tokenization)
        name_tokens = asset.name.lower().split()
        for token in name_tokens:
            self._name_index[token].add(asset.asset_id)
        
        # Tag index
        for tag in asset.tags:
            self._tag_index[tag].add(asset.asset_id)
        
        # Owner index
        if asset.owner:
            self._owner_index[asset.owner].add(asset.asset_id)
        
        # Classification index
        self._classification_index[asset.classification].add(asset.asset_id)
    
    def get_catalog_statistics(self) -> Dict[str, Any]:
        """Get catalog statistics"""
        
        total_assets = len(self.assets)
        
        # Classification distribution
        classification_dist = {}
        for classification in DataClassification:
            classification_dist[classification.value] = len(
                self._classification_index.get(classification, set())
            )
        
        # Asset type distribution
        type_dist = defaultdict(int)
        for asset in self.assets.values():
            type_dist[asset.asset_type] += 1
        
        # Quality distribution
        quality_dist = defaultdict(int)
        for asset in self.assets.values():
            quality_dist[asset.quality_level.value] += 1
        
        # PII/PHI counts
        pii_count = sum(1 for asset in self.assets.values() if asset.contains_pii)
        phi_count = sum(1 for asset in self.assets.values() if asset.contains_phi)
        
        return {
            'total_assets': total_assets,
            'total_glossary_terms': len(self.business_glossary),
            'classification_distribution': classification_dist,
            'asset_type_distribution': dict(type_dist),
            'quality_distribution': dict(quality_dist),
            'privacy_sensitive_data': {
                'assets_with_pii': pii_count,
                'assets_with_phi': phi_count,
                'percentage_sensitive': (pii_count + phi_count) / total_assets * 100 if total_assets > 0 else 0
            }
        }


class DataLineageTracker:
    """Advanced data lineage tracking system"""
    
    def __init__(self, catalog: DataCatalog):
        self.logger = get_logger(__name__)
        self.catalog = catalog
        
        # Lineage storage
        self.lineage_events: List[LineageEvent] = []
        self.lineage_graph = nx.DiGraph()
        self.asset_lineage: Dict[str, DataLineage] = {}
        
        # Event processing queue
        self.event_queue: deque = deque()
        
    def record_lineage_event(
        self,
        event_type: LineageEventType,
        target_asset_id: str,
        source_asset_id: Optional[str] = None,
        operation: str = "",
        operation_details: Dict[str, Any] = None,
        user_id: Optional[str] = None,
        system_id: str = "unknown",
        session_id: Optional[str] = None,
        transformation_logic: Optional[str] = None
    ) -> str:
        """Record a data lineage event"""
        
        event = LineageEvent(
            event_id=str(uuid.uuid4()),
            timestamp=datetime.now(),
            event_type=event_type,
            source_asset_id=source_asset_id,
            target_asset_id=target_asset_id,
            operation=operation,
            operation_details=operation_details or {},
            user_id=user_id,
            system_id=system_id,
            session_id=session_id,
            transformation_logic=transformation_logic
        )
        
        # Store event
        self.lineage_events.append(event)
        self.event_queue.append(event)
        
        # Update lineage graph
        self._update_lineage_graph(event)
        
        # Update asset lineage
        self._update_asset_lineage(event)
        
        self.logger.info(f"Recorded lineage event: {event_type.value} for {target_asset_id}")
        return event.event_id
    
    def _update_lineage_graph(self, event: LineageEvent):
        """Update the lineage graph with new event"""
        
        # Add target asset to graph if not present
        if not self.lineage_graph.has_node(event.target_asset_id):
            self.lineage_graph.add_node(event.target_asset_id)
        
        # Add source asset and edge if applicable
        if event.source_asset_id:
            if not self.lineage_graph.has_node(event.source_asset_id):
                self.lineage_graph.add_node(event.source_asset_id)
            
            # Add or update edge
            self.lineage_graph.add_edge(
                event.source_asset_id,
                event.target_asset_id,
                event_type=event.event_type.value,
                operation=event.operation,
                timestamp=event.timestamp,
                event_id=event.event_id
            )
    
    def _update_asset_lineage(self, event: LineageEvent):
        """Update asset-specific lineage information"""
        
        # Update target asset lineage
        if event.target_asset_id not in self.asset_lineage:
            self.asset_lineage[event.target_asset_id] = DataLineage(
                lineage_id=str(uuid.uuid4()),
                asset_id=event.target_asset_id
            )
        
        target_lineage = self.asset_lineage[event.target_asset_id]
        target_lineage.lineage_events.append(event.event_id)
        target_lineage.updated_at = datetime.now()
        
        # Update upstream/downstream relationships
        if event.source_asset_id:
            target_lineage.upstream_assets.add(event.source_asset_id)
            
            # Update source asset's downstream
            if event.source_asset_id not in self.asset_lineage:
                self.asset_lineage[event.source_asset_id] = DataLineage(
                    lineage_id=str(uuid.uuid4()),
                    asset_id=event.source_asset_id
                )
            
            source_lineage = self.asset_lineage[event.source_asset_id]
            source_lineage.downstream_assets.add(event.target_asset_id)
            source_lineage.updated_at = datetime.now()
    
    def get_upstream_lineage(
        self, 
        asset_id: str, 
        max_depth: int = 10
    ) -> List[Dict[str, Any]]:
        """Get upstream lineage for an asset"""
        
        if not self.lineage_graph.has_node(asset_id):
            return []
        
        upstream_nodes = []
        visited = set()
        queue = deque([(asset_id, 0)])
        
        while queue:
            current_asset, depth = queue.popleft()
            
            if depth >= max_depth or current_asset in visited:
                continue
            
            visited.add(current_asset)
            
            # Get predecessors (upstream assets)
            predecessors = list(self.lineage_graph.predecessors(current_asset))
            
            for pred in predecessors:
                edge_data = self.lineage_graph.get_edge_data(pred, current_asset)
                
                upstream_nodes.append({
                    'asset_id': pred,
                    'target_asset_id': current_asset,
                    'depth': depth + 1,
                    'relationship_type': edge_data.get('event_type', 'unknown'),
                    'operation': edge_data.get('operation', ''),
                    'timestamp': edge_data.get('timestamp').isoformat() if edge_data.get('timestamp') else None
                })
                
                queue.append((pred, depth + 1))
        
        return upstream_nodes
    
    def get_downstream_lineage(
        self, 
        asset_id: str, 
        max_depth: int = 10
    ) -> List[Dict[str, Any]]:
        """Get downstream lineage for an asset"""
        
        if not self.lineage_graph.has_node(asset_id):
            return []
        
        downstream_nodes = []
        visited = set()
        queue = deque([(asset_id, 0)])
        
        while queue:
            current_asset, depth = queue.popleft()
            
            if depth >= max_depth or current_asset in visited:
                continue
            
            visited.add(current_asset)
            
            # Get successors (downstream assets)
            successors = list(self.lineage_graph.successors(current_asset))
            
            for succ in successors:
                edge_data = self.lineage_graph.get_edge_data(current_asset, succ)
                
                downstream_nodes.append({
                    'asset_id': succ,
                    'source_asset_id': current_asset,
                    'depth': depth + 1,
                    'relationship_type': edge_data.get('event_type', 'unknown'),
                    'operation': edge_data.get('operation', ''),
                    'timestamp': edge_data.get('timestamp').isoformat() if edge_data.get('timestamp') else None
                })
                
                queue.append((succ, depth + 1))
        
        return downstream_nodes
    
    def get_lineage_impact_analysis(self, asset_id: str) -> Dict[str, Any]:
        """Analyze the impact of changes to an asset"""
        
        upstream = self.get_upstream_lineage(asset_id)
        downstream = self.get_downstream_lineage(asset_id)
        
        # Calculate impact metrics
        direct_upstream = len([u for u in upstream if u['depth'] == 1])
        direct_downstream = len([d for d in downstream if d['depth'] == 1])
        total_upstream = len(upstream)
        total_downstream = len(downstream)
        
        # Identify critical paths (chains of dependencies)
        critical_paths = self._identify_critical_paths(asset_id)
        
        return {
            'asset_id': asset_id,
            'impact_summary': {
                'direct_upstream_dependencies': direct_upstream,
                'direct_downstream_dependencies': direct_downstream,
                'total_upstream_dependencies': total_upstream,
                'total_downstream_dependencies': total_downstream,
                'impact_radius': max(total_upstream, total_downstream)
            },
            'upstream_assets': upstream,
            'downstream_assets': downstream,
            'critical_paths': critical_paths,
            'risk_assessment': {
                'impact_level': self._calculate_impact_level(total_upstream + total_downstream),
                'change_risk': 'high' if total_downstream > 10 else 'medium' if total_downstream > 5 else 'low'
            }
        }
    
    def _identify_critical_paths(self, asset_id: str) -> List[List[str]]:
        """Identify critical dependency paths"""
        
        critical_paths = []
        
        try:
            # Find all simple paths from asset to leaf nodes
            for node in self.lineage_graph.nodes():
                if self.lineage_graph.out_degree(node) == 0:  # Leaf node
                    try:
                        paths = list(nx.all_simple_paths(
                            self.lineage_graph, asset_id, node, cutoff=10
                        ))
                        critical_paths.extend(paths)
                    except nx.NetworkXNoPath:
                        continue
        
        except Exception as e:
            self.logger.warning(f"Error identifying critical paths: {e}")
        
        return critical_paths[:10]  # Limit to top 10 paths
    
    def _calculate_impact_level(self, dependency_count: int) -> str:
        """Calculate impact level based on dependency count"""
        
        if dependency_count > 50:
            return "critical"
        elif dependency_count > 20:
            return "high"
        elif dependency_count > 5:
            return "medium"
        else:
            return "low"
    
    def get_lineage_statistics(self) -> Dict[str, Any]:
        """Get lineage tracking statistics"""
        
        total_events = len(self.lineage_events)
        total_assets_in_lineage = len(self.asset_lineage)
        
        # Event type distribution
        event_type_dist = defaultdict(int)
        for event in self.lineage_events:
            event_type_dist[event.event_type.value] += 1
        
        # Graph statistics
        total_nodes = self.lineage_graph.number_of_nodes()
        total_edges = self.lineage_graph.number_of_edges()
        
        # Complexity metrics
        avg_upstream = sum(len(lineage.upstream_assets) for lineage in self.asset_lineage.values()) / total_assets_in_lineage if total_assets_in_lineage > 0 else 0
        avg_downstream = sum(len(lineage.downstream_assets) for lineage in self.asset_lineage.values()) / total_assets_in_lineage if total_assets_in_lineage > 0 else 0
        
        return {
            'total_lineage_events': total_events,
            'assets_with_lineage': total_assets_in_lineage,
            'graph_statistics': {
                'total_nodes': total_nodes,
                'total_edges': total_edges,
                'average_upstream_dependencies': round(avg_upstream, 2),
                'average_downstream_dependencies': round(avg_downstream, 2)
            },
            'event_type_distribution': dict(event_type_dist),
            'lineage_coverage': {
                'assets_in_catalog': len(self.catalog.assets),
                'assets_with_lineage': total_assets_in_lineage,
                'coverage_percentage': (total_assets_in_lineage / len(self.catalog.assets) * 100) if len(self.catalog.assets) > 0 else 0
            }
        }


class DataRetentionManager:
    """Data retention and lifecycle management"""
    
    def __init__(self, catalog: DataCatalog, audit_logger: Optional[AuditLogger] = None):
        self.logger = get_logger(__name__)
        self.catalog = catalog
        self.audit_logger = audit_logger or AuditLogger()
        
        self.retention_policies: Dict[str, DataRetentionPolicy] = {}
        self._initialize_default_policies()
    
    def _initialize_default_policies(self):
        """Initialize default retention policies"""
        
        default_policies = [
            DataRetentionPolicy(
                policy_id="gdpr_personal_data",
                name="GDPR Personal Data Retention",
                description="Retain personal data for maximum 7 years unless legal obligation requires longer",
                data_categories=["personal_data", "pii"],
                retention_period=timedelta(days=2555),  # 7 years
                disposal_method="delete",
                legal_basis="GDPR Article 5(1)(e)"
            ),
            DataRetentionPolicy(
                policy_id="financial_records",
                name="Financial Records Retention",
                description="Retain financial records for 7 years for SOX compliance",
                data_categories=["financial_data", "transaction_data"],
                retention_period=timedelta(days=2555),  # 7 years
                disposal_method="archive",
                legal_basis="SOX Section 802"
            ),
            DataRetentionPolicy(
                policy_id="audit_logs",
                name="Audit Log Retention",
                description="Retain audit logs for compliance monitoring",
                data_categories=["audit_log", "security_log"],
                retention_period=timedelta(days=2555),  # 7 years
                disposal_method="archive",
                legal_basis="Compliance requirement"
            ),
            DataRetentionPolicy(
                policy_id="operational_data",
                name="Operational Data Retention",
                description="Standard retention for operational data",
                data_categories=["operational_data"],
                retention_period=timedelta(days=1095),  # 3 years
                disposal_method="delete",
                legal_basis="Business requirement"
            )
        ]
        
        for policy in default_policies:
            self.retention_policies[policy.policy_id] = policy
    
    def add_retention_policy(self, policy: DataRetentionPolicy) -> str:
        """Add a new retention policy"""
        
        if not policy.policy_id:
            policy.policy_id = str(uuid.uuid4())
        
        self.retention_policies[policy.policy_id] = policy
        self.logger.info(f"Added retention policy: {policy.name}")
        return policy.policy_id
    
    def apply_retention_policy(self, asset_id: str, policy_id: str) -> bool:
        """Apply retention policy to an asset"""
        
        if asset_id not in self.catalog.assets or policy_id not in self.retention_policies:
            return False
        
        asset = self.catalog.assets[asset_id]
        policy = self.retention_policies[policy_id]
        
        asset.retention_period = policy.retention_period
        asset.updated_at = datetime.now()
        
        # Audit log
        self.audit_logger.log_audit_event(
            user_id="system",
            action=ActionType.UPDATE,
            resource_type="data_asset_retention",
            resource_id=asset_id,
            new_value={
                "policy_applied": policy_id,
                "retention_period_days": policy.retention_period.days,
                "disposal_method": policy.disposal_method
            }
        )
        
        return True
    
    def check_retention_compliance(self) -> List[Dict[str, Any]]:
        """Check retention compliance for all assets"""
        
        compliance_issues = []
        now = datetime.now()
        
        for asset_id, asset in self.catalog.assets.items():
            if not asset.retention_period:
                compliance_issues.append({
                    'asset_id': asset_id,
                    'asset_name': asset.name,
                    'issue': 'No retention policy assigned',
                    'severity': 'medium'
                })
                continue
            
            # Calculate retention deadline
            retention_deadline = asset.created_at + asset.retention_period
            
            if now > retention_deadline:
                days_overdue = (now - retention_deadline).days
                compliance_issues.append({
                    'asset_id': asset_id,
                    'asset_name': asset.name,
                    'issue': f'Retention period exceeded by {days_overdue} days',
                    'severity': 'high' if days_overdue > 30 else 'medium',
                    'retention_deadline': retention_deadline.isoformat(),
                    'days_overdue': days_overdue
                })
        
        return compliance_issues
    
    def get_assets_for_disposal(self, grace_period_days: int = 30) -> List[Dict[str, Any]]:
        """Get assets that should be disposed of"""
        
        disposal_candidates = []
        now = datetime.now()
        grace_period = timedelta(days=grace_period_days)
        
        for asset_id, asset in self.catalog.assets.items():
            if not asset.retention_period:
                continue
            
            retention_deadline = asset.created_at + asset.retention_period
            disposal_deadline = retention_deadline + grace_period
            
            if now > disposal_deadline:
                # Find applicable policy
                applicable_policy = None
                for policy in self.retention_policies.values():
                    if any(cat in asset.tags for cat in policy.data_categories):
                        applicable_policy = policy
                        break
                
                disposal_candidates.append({
                    'asset_id': asset_id,
                    'asset_name': asset.name,
                    'location': asset.location,
                    'retention_deadline': retention_deadline.isoformat(),
                    'disposal_method': applicable_policy.disposal_method if applicable_policy else 'delete',
                    'days_past_deadline': (now - retention_deadline).days,
                    'size_bytes': asset.size_bytes,
                    'contains_pii': asset.contains_pii,
                    'classification': asset.classification.value
                })
        
        return disposal_candidates


class PrivacyGovernanceManager:
    """Privacy governance and impact assessment manager"""
    
    def __init__(self, catalog: DataCatalog):
        self.logger = get_logger(__name__)
        self.catalog = catalog
        self.privacy_assessments: Dict[str, PrivacyImpactAssessment] = {}
    
    def conduct_privacy_impact_assessment(
        self,
        asset_id: str,
        assessor: str,
        data_types: List[str],
        purposes: List[str],
        subjects: List[str],
        third_party_sharing: bool = False,
        international_transfers: bool = False
    ) -> str:
        """Conduct Privacy Impact Assessment for an asset"""
        
        if asset_id not in self.catalog.assets:
            raise ValueError(f"Asset {asset_id} not found in catalog")
        
        # Assess privacy risks
        privacy_risks = self._assess_privacy_risks(
            data_types, purposes, subjects, third_party_sharing, international_transfers
        )
        
        # Determine overall risk level
        risk_level = self._calculate_overall_risk_level(privacy_risks)
        
        # Generate mitigation measures
        mitigation_measures = self._generate_mitigation_measures(privacy_risks)
        
        pia = PrivacyImpactAssessment(
            pia_id=str(uuid.uuid4()),
            asset_id=asset_id,
            assessment_date=datetime.now(),
            assessor=assessor,
            data_types_processed=data_types,
            processing_purposes=purposes,
            data_subjects=subjects,
            third_party_sharing=third_party_sharing,
            international_transfers=international_transfers,
            privacy_risks=privacy_risks,
            mitigation_measures=mitigation_measures,
            residual_risk_level=risk_level,
            legal_basis="Legitimate interest",  # Default, should be specified
            consent_required=self._requires_consent(data_types, purposes),
            legitimate_interest=True,
            review_date=datetime.now() + timedelta(days=365)  # Annual review
        )
        
        self.privacy_assessments[pia.pia_id] = pia
        
        # Update asset privacy flags
        asset = self.catalog.assets[asset_id]
        asset.privacy_impact_level = risk_level
        asset.contains_pii = any("personal" in dt.lower() for dt in data_types)
        asset.contains_phi = any("health" in dt.lower() for dt in data_types)
        asset.gdpr_applicable = asset.contains_pii or asset.contains_phi
        
        self.logger.info(f"Completed PIA for asset {asset_id}: Risk level {risk_level.value}")
        return pia.pia_id
    
    def _assess_privacy_risks(
        self,
        data_types: List[str],
        purposes: List[str],
        subjects: List[str],
        third_party_sharing: bool,
        international_transfers: bool
    ) -> List[Dict[str, Any]]:
        """Assess privacy risks based on processing characteristics"""
        
        risks = []
        
        # Data sensitivity risk
        sensitive_data_types = ["ssn", "credit_card", "health", "biometric", "genetic"]
        if any(sdt in " ".join(data_types).lower() for sdt in sensitive_data_types):
            risks.append({
                'risk_type': 'High sensitivity data processing',
                'likelihood': 'medium',
                'impact': 'high',
                'description': 'Processing of highly sensitive personal data increases privacy risks'
            })
        
        # Purpose limitation risk
        broad_purposes = ["marketing", "profiling", "analytics", "research"]
        if any(bp in " ".join(purposes).lower() for bp in broad_purposes):
            risks.append({
                'risk_type': 'Purpose creep',
                'likelihood': 'medium',
                'impact': 'medium',
                'description': 'Broad processing purposes may lead to unauthorized use'
            })
        
        # Third-party sharing risk
        if third_party_sharing:
            risks.append({
                'risk_type': 'Third-party data sharing',
                'likelihood': 'high',
                'impact': 'high',
                'description': 'Sharing personal data with third parties increases exposure risk'
            })
        
        # International transfer risk
        if international_transfers:
            risks.append({
                'risk_type': 'International data transfers',
                'likelihood': 'medium',
                'impact': 'high',
                'description': 'Cross-border data transfers may lack adequate protection'
            })
        
        # Vulnerable populations risk
        vulnerable_subjects = ["children", "elderly", "patients", "employees"]
        if any(vs in " ".join(subjects).lower() for vs in vulnerable_subjects):
            risks.append({
                'risk_type': 'Vulnerable data subjects',
                'likelihood': 'medium',
                'impact': 'high',
                'description': 'Processing data of vulnerable individuals requires special protection'
            })
        
        return risks
    
    def _calculate_overall_risk_level(self, risks: List[Dict[str, Any]]) -> PrivacyImpactLevel:
        """Calculate overall privacy impact level"""
        
        if not risks:
            return PrivacyImpactLevel.NO_IMPACT
        
        high_impact_risks = [r for r in risks if r['impact'] == 'high']
        medium_impact_risks = [r for r in risks if r['impact'] == 'medium']
        
        if len(high_impact_risks) >= 3:
            return PrivacyImpactLevel.CRITICAL_IMPACT
        elif len(high_impact_risks) >= 2:
            return PrivacyImpactLevel.HIGH_IMPACT
        elif len(high_impact_risks) >= 1 or len(medium_impact_risks) >= 3:
            return PrivacyImpactLevel.MODERATE_IMPACT
        elif len(medium_impact_risks) >= 1:
            return PrivacyImpactLevel.LOW_IMPACT
        else:
            return PrivacyImpactLevel.NO_IMPACT
    
    def _generate_mitigation_measures(self, risks: List[Dict[str, Any]]) -> List[str]:
        """Generate mitigation measures for identified risks"""
        
        measures = []
        
        risk_types = [r['risk_type'] for r in risks]
        
        if 'High sensitivity data processing' in risk_types:
            measures.extend([
                "Implement strong encryption for sensitive data",
                "Restrict access to authorized personnel only",
                "Implement data masking for non-production environments"
            ])
        
        if 'Third-party data sharing' in risk_types:
            measures.extend([
                "Establish data processing agreements with third parties",
                "Conduct due diligence on third-party security practices",
                "Implement data minimization for shared data"
            ])
        
        if 'International data transfers' in risk_types:
            measures.extend([
                "Implement Standard Contractual Clauses (SCCs)",
                "Conduct Transfer Impact Assessments",
                "Use appropriate technical safeguards for transfers"
            ])
        
        if 'Vulnerable data subjects' in risk_types:
            measures.extend([
                "Implement enhanced consent mechanisms",
                "Provide clear privacy notices in accessible formats",
                "Establish special procedures for vulnerable subject rights"
            ])
        
        # General measures
        measures.extend([
            "Conduct regular privacy training for staff",
            "Implement privacy by design principles",
            "Establish data breach response procedures",
            "Conduct regular privacy impact assessments"
        ])
        
        return list(set(measures))  # Remove duplicates
    
    def _requires_consent(self, data_types: List[str], purposes: List[str]) -> bool:
        """Determine if consent is required for processing"""
        
        # Simplified logic - in practice this would be more complex
        marketing_purposes = ["marketing", "advertising", "profiling"]
        sensitive_data = ["biometric", "genetic", "health", "political"]
        
        if any(mp in " ".join(purposes).lower() for mp in marketing_purposes):
            return True
        
        if any(sd in " ".join(data_types).lower() for sd in sensitive_data):
            return True
        
        return False
    
    def get_privacy_dashboard(self) -> Dict[str, Any]:
        """Get privacy governance dashboard"""
        
        total_assessments = len(self.privacy_assessments)
        
        # Risk level distribution
        risk_distribution = defaultdict(int)
        for pia in self.privacy_assessments.values():
            risk_distribution[pia.residual_risk_level.value] += 1
        
        # Assets requiring attention
        high_risk_assets = [
            pia.asset_id for pia in self.privacy_assessments.values()
            if pia.residual_risk_level in [PrivacyImpactLevel.HIGH_IMPACT, PrivacyImpactLevel.CRITICAL_IMPACT]
        ]
        
        # Review status
        now = datetime.now()
        overdue_reviews = [
            pia for pia in self.privacy_assessments.values()
            if pia.review_date < now
        ]
        
        return {
            'total_privacy_assessments': total_assessments,
            'risk_level_distribution': dict(risk_distribution),
            'high_risk_assets': len(high_risk_assets),
            'overdue_reviews': len(overdue_reviews),
            'compliance_metrics': {
                'assets_with_pia': total_assessments,
                'percentage_assessed': (total_assessments / len(self.catalog.assets) * 100) if len(self.catalog.assets) > 0 else 0
            }
        }


class DataGovernanceOrchestrator:
    """Main orchestrator for comprehensive data governance"""
    
    def __init__(self, audit_logger: Optional[AuditLogger] = None):
        self.logger = get_logger(__name__)
        self.audit_logger = audit_logger or AuditLogger()
        
        # Initialize components
        self.catalog = DataCatalog(audit_logger)
        self.lineage_tracker = DataLineageTracker(self.catalog)
        self.retention_manager = DataRetentionManager(self.catalog, audit_logger)
        self.privacy_manager = PrivacyGovernanceManager(self.catalog)
    
    def get_comprehensive_dashboard(self) -> Dict[str, Any]:
        """Get comprehensive data governance dashboard"""
        
        return {
            'catalog_statistics': self.catalog.get_catalog_statistics(),
            'lineage_statistics': self.lineage_tracker.get_lineage_statistics(),
            'retention_compliance': {
                'issues': len(self.retention_manager.check_retention_compliance()),
                'assets_for_disposal': len(self.retention_manager.get_assets_for_disposal())
            },
            'privacy_governance': self.privacy_manager.get_privacy_dashboard(),
            'overall_governance_score': self._calculate_governance_score()
        }
    
    def _calculate_governance_score(self) -> float:
        """Calculate overall data governance score"""
        
        score = 0.0
        
        # Catalog completeness (25%)
        catalog_stats = self.catalog.get_catalog_statistics()
        if catalog_stats['total_assets'] > 0:
            catalog_score = min(1.0, catalog_stats['total_assets'] / 100)  # Normalize to assets count
            score += catalog_score * 0.25
        
        # Lineage coverage (25%)
        lineage_stats = self.lineage_tracker.get_lineage_statistics()
        lineage_coverage = lineage_stats['lineage_coverage']['coverage_percentage'] / 100
        score += lineage_coverage * 0.25
        
        # Retention compliance (25%)
        retention_issues = self.retention_manager.check_retention_compliance()
        retention_score = max(0.0, 1.0 - (len(retention_issues) / max(1, len(self.catalog.assets))))
        score += retention_score * 0.25
        
        # Privacy governance (25%)
        privacy_stats = self.privacy_manager.get_privacy_dashboard()
        privacy_score = privacy_stats['compliance_metrics']['percentage_assessed'] / 100
        score += privacy_score * 0.25
        
        return round(score, 2)


# Global governance orchestrator
_governance_orchestrator: Optional[DataGovernanceOrchestrator] = None

def get_governance_orchestrator() -> DataGovernanceOrchestrator:
    """Get global data governance orchestrator instance"""
    global _governance_orchestrator
    if _governance_orchestrator is None:
        _governance_orchestrator = DataGovernanceOrchestrator()
    return _governance_orchestrator