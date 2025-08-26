"""
Data Lineage Tracker
Advanced data lineage tracking and impact analysis across ETL pipelines.
"""
from __future__ import annotations

import json
import re
import threading
from collections import defaultdict, deque
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

from core.logging import get_logger


class LineageType(Enum):
    """Types of lineage relationships"""
    READ = "read"
    WRITE = "write" 
    TRANSFORM = "transform"
    DERIVE = "derive"
    COPY = "copy"
    AGGREGATE = "aggregate"
    JOIN = "join"
    FILTER = "filter"
    UNION = "union"


class LineageCapture(Enum):
    """Methods of lineage capture"""
    AUTOMATIC = "automatic"
    MANUAL = "manual"
    PARSED = "parsed"
    INFERRED = "inferred"


@dataclass
class LineageEdge:
    """Single lineage relationship between two assets"""
    edge_id: str
    source_asset_id: str
    target_asset_id: str
    lineage_type: LineageType
    transformation_logic: str | None = None
    field_mappings: dict[str, list[str]] = field(default_factory=dict)
    
    # Context information
    pipeline_id: str | None = None
    job_id: str | None = None
    execution_id: str | None = None
    
    # Metadata
    capture_method: LineageCapture = LineageCapture.AUTOMATIC
    confidence_score: float = 1.0
    created_at: datetime = field(default_factory=datetime.now)
    created_by: str | None = None
    
    # Technical details
    query: str | None = None
    script_path: str | None = None
    function_name: str | None = None
    
    # Quality information
    data_quality_impact: dict[str, Any] = field(default_factory=dict)
    business_impact: str | None = None
    
    tags: list[str] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class LineageGraph:
    """Complete lineage graph for an asset or pipeline"""
    root_asset_id: str
    nodes: dict[str, dict[str, Any]] = field(default_factory=dict)
    edges: list[LineageEdge] = field(default_factory=list)
    depth: int = 0
    total_assets: int = 0
    
    # Analysis results
    upstream_count: int = 0
    downstream_count: int = 0
    transformation_count: int = 0
    critical_path: list[str] = field(default_factory=list)
    
    # Impact analysis
    impact_radius: int = 0
    affected_systems: set[str] = field(default_factory=set)
    business_processes: set[str] = field(default_factory=set)


@dataclass
class ImpactAnalysis:
    """Impact analysis results"""
    target_asset_id: str
    change_type: str
    impact_scope: str = "unknown"
    
    # Affected assets
    directly_affected: list[str] = field(default_factory=list)
    indirectly_affected: list[str] = field(default_factory=list)
    
    # Business impact
    affected_reports: list[str] = field(default_factory=list)
    affected_dashboards: list[str] = field(default_factory=list)
    affected_pipelines: list[str] = field(default_factory=list)
    
    # Risk assessment
    risk_level: str = "low"
    estimated_fix_time: int | None = None  # Hours
    recommended_actions: list[str] = field(default_factory=list)
    
    analysis_timestamp: datetime = field(default_factory=datetime.now)


class LineageTracker:
    """
    Advanced Data Lineage Tracker
    
    Provides comprehensive lineage tracking including:
    - Automatic lineage capture from SQL queries
    - Manual lineage registration
    - Impact analysis for changes
    - Lineage visualization and exploration
    - Field-level lineage tracking
    - Cross-system lineage mapping
    """

    def __init__(self, storage_path: Path | None = None):
        self.logger = get_logger(self.__class__.__name__)
        
        # Storage
        self.storage_path = storage_path or Path("./lineage")
        self.edges_path = self.storage_path / "edges"
        self.graphs_path = self.storage_path / "graphs"
        self.analysis_path = self.storage_path / "analysis"
        
        self._ensure_directories()
        
        # In-memory storage for performance
        self.lineage_edges: dict[str, LineageEdge] = {}
        self.asset_edges: dict[str, list[str]] = defaultdict(list)  # asset_id -> edge_ids
        self.pipeline_edges: dict[str, list[str]] = defaultdict(list)  # pipeline_id -> edge_ids
        
        # Thread safety
        self._lock = threading.Lock()
        
        # SQL parsing patterns
        self._init_sql_patterns()
        
        # Load existing lineage
        self._load_lineage_data()

    def _ensure_directories(self):
        """Ensure storage directories exist"""
        for path in [self.storage_path, self.edges_path, self.graphs_path, self.analysis_path]:
            path.mkdir(parents=True, exist_ok=True)

    def _init_sql_patterns(self):
        """Initialize SQL parsing patterns for lineage extraction"""
        self.sql_patterns = {
            'select_from': re.compile(
                r'SELECT\s+.*?\s+FROM\s+(\w+(?:\.\w+)*)', 
                re.IGNORECASE | re.MULTILINE
            ),
            'insert_into': re.compile(
                r'INSERT\s+INTO\s+(\w+(?:\.\w+)*)', 
                re.IGNORECASE
            ),
            'update_table': re.compile(
                r'UPDATE\s+(\w+(?:\.\w+)*)', 
                re.IGNORECASE
            ),
            'create_table_as': re.compile(
                r'CREATE\s+TABLE\s+(\w+(?:\.\w+)*)\s+AS\s+SELECT\s+.*?\s+FROM\s+(\w+(?:\.\w+)*)', 
                re.IGNORECASE | re.MULTILINE
            ),
            'join_tables': re.compile(
                r'JOIN\s+(\w+(?:\.\w+)*)', 
                re.IGNORECASE
            ),
            'union_with': re.compile(
                r'UNION\s+(?:ALL\s+)?SELECT\s+.*?\s+FROM\s+(\w+(?:\.\w+)*)', 
                re.IGNORECASE | re.MULTILINE
            )
        }

    def _load_lineage_data(self):
        """Load existing lineage data from storage"""
        try:
            # Load edges
            for edge_file in self.edges_path.glob("*.json"):
                try:
                    with open(edge_file) as f:
                        edge_data = json.load(f)
                        edge = self._dict_to_edge(edge_data)
                        self.lineage_edges[edge.edge_id] = edge
                        
                        # Update indexes
                        self.asset_edges[edge.source_asset_id].append(edge.edge_id)
                        self.asset_edges[edge.target_asset_id].append(edge.edge_id)
                        
                        if edge.pipeline_id:
                            self.pipeline_edges[edge.pipeline_id].append(edge.edge_id)
                            
                except Exception as e:
                    self.logger.error(f"Failed to load lineage edge {edge_file}: {e}")
            
            self.logger.info(f"Loaded {len(self.lineage_edges)} lineage edges")
            
        except Exception as e:
            self.logger.error(f"Failed to load lineage data: {e}")

    def register_lineage(
        self,
        source_asset_id: str,
        target_asset_id: str,
        lineage_type: LineageType,
        transformation_logic: str | None = None,
        field_mappings: dict[str, list[str]] | None = None,
        **kwargs
    ) -> str:
        """
        Register a lineage relationship
        
        Args:
            source_asset_id: Source asset identifier
            target_asset_id: Target asset identifier  
            lineage_type: Type of lineage relationship
            transformation_logic: Optional transformation description
            field_mappings: Field-level mappings (source_field -> [target_fields])
            **kwargs: Additional metadata
        
        Returns:
            Lineage edge ID
        """
        try:
            with self._lock:
                edge_id = f"{source_asset_id}_to_{target_asset_id}_{int(datetime.now().timestamp())}"
                
                edge = LineageEdge(
                    edge_id=edge_id,
                    source_asset_id=source_asset_id,
                    target_asset_id=target_asset_id,
                    lineage_type=lineage_type,
                    transformation_logic=transformation_logic,
                    field_mappings=field_mappings or {},
                    **kwargs
                )
                
                # Store edge
                self.lineage_edges[edge_id] = edge
                
                # Update indexes
                self.asset_edges[source_asset_id].append(edge_id)
                self.asset_edges[target_asset_id].append(edge_id)
                
                if edge.pipeline_id:
                    self.pipeline_edges[edge.pipeline_id].append(edge_id)
                
                # Persist to storage
                self._persist_edge(edge)
                
                self.logger.info(f"Registered lineage: {source_asset_id} -> {target_asset_id}")
                return edge_id
                
        except Exception as e:
            self.logger.error(f"Failed to register lineage: {e}")
            raise

    def parse_sql_lineage(
        self,
        sql_query: str,
        target_asset_id: str | None = None,
        pipeline_id: str | None = None,
        **kwargs
    ) -> list[str]:
        """
        Parse SQL query and extract lineage relationships
        
        Args:
            sql_query: SQL query to parse
            target_asset_id: Target asset being created/updated
            pipeline_id: Pipeline identifier
            **kwargs: Additional metadata
        
        Returns:
            List of created lineage edge IDs
        """
        try:
            edge_ids = []
            
            # Normalize SQL
            sql_normalized = self._normalize_sql(sql_query)
            
            # Extract different types of relationships
            relationships = self._extract_sql_relationships(sql_normalized)
            
            for relationship in relationships:
                # Create lineage edge
                edge_id = self.register_lineage(
                    source_asset_id=relationship['source'],
                    target_asset_id=relationship.get('target', target_asset_id),
                    lineage_type=relationship['type'],
                    transformation_logic=relationship.get('logic'),
                    query=sql_query,
                    pipeline_id=pipeline_id,
                    capture_method=LineageCapture.PARSED,
                    **kwargs
                )
                
                edge_ids.append(edge_id)
            
            self.logger.info(f"Parsed {len(edge_ids)} lineage relationships from SQL")
            return edge_ids
            
        except Exception as e:
            self.logger.error(f"SQL lineage parsing failed: {e}")
            return []

    def build_lineage_graph(
        self,
        asset_id: str,
        direction: str = "both",
        max_depth: int = 5,
        include_field_mappings: bool = False
    ) -> LineageGraph:
        """
        Build comprehensive lineage graph for an asset
        
        Args:
            asset_id: Root asset identifier
            direction: "upstream", "downstream", or "both"
            max_depth: Maximum traversal depth
            include_field_mappings: Whether to include field-level lineage
        
        Returns:
            Complete lineage graph
        """
        try:
            with self._lock:
                graph = LineageGraph(root_asset_id=asset_id)
                
                # Build graph using BFS traversal
                if direction in ["upstream", "both"]:
                    upstream_graph = self._traverse_lineage(
                        asset_id, "upstream", max_depth
                    )
                    graph.nodes.update(upstream_graph['nodes'])
                    graph.edges.extend(upstream_graph['edges'])
                    graph.upstream_count = len(upstream_graph['nodes'])
                
                if direction in ["downstream", "both"]:
                    downstream_graph = self._traverse_lineage(
                        asset_id, "downstream", max_depth
                    )
                    graph.nodes.update(downstream_graph['nodes'])
                    graph.edges.extend(downstream_graph['edges'])
                    graph.downstream_count = len(downstream_graph['nodes'])
                
                # Calculate graph statistics
                graph.total_assets = len(graph.nodes)
                graph.transformation_count = len([
                    e for e in graph.edges 
                    if e.lineage_type == LineageType.TRANSFORM
                ])
                
                # Find critical path (longest path)
                graph.critical_path = self._find_critical_path(graph)
                
                # Calculate impact radius
                graph.impact_radius = max(graph.upstream_count, graph.downstream_count)
                
                # Identify affected systems
                graph.affected_systems = {
                    node.get('system', 'unknown') 
                    for node in graph.nodes.values()
                }
                
                return graph
                
        except Exception as e:
            self.logger.error(f"Failed to build lineage graph for {asset_id}: {e}")
            return LineageGraph(root_asset_id=asset_id)

    def analyze_impact(
        self,
        asset_id: str,
        change_type: str,
        change_details: dict[str, Any] | None = None
    ) -> ImpactAnalysis:
        """
        Analyze potential impact of changes to an asset
        
        Args:
            asset_id: Asset being changed
            change_type: Type of change (schema, location, etc.)
            change_details: Additional change details
        
        Returns:
            Impact analysis results
        """
        try:
            analysis = ImpactAnalysis(
                target_asset_id=asset_id,
                change_type=change_type
            )
            
            # Build downstream lineage
            downstream_graph = self._traverse_lineage(asset_id, "downstream", max_depth=10)
            
            # Direct impact (immediate downstream assets)
            for edge in downstream_graph['edges']:
                if edge.source_asset_id == asset_id:
                    analysis.directly_affected.append(edge.target_asset_id)
            
            # Indirect impact (further downstream)
            all_downstream = set(downstream_graph['nodes'].keys())
            direct_set = set(analysis.directly_affected)
            analysis.indirectly_affected = list(all_downstream - direct_set - {asset_id})
            
            # Categorize affected assets
            for node_id, node_info in downstream_graph['nodes'].items():
                asset_type = node_info.get('type', 'unknown')
                
                if asset_type == 'report':
                    analysis.affected_reports.append(node_id)
                elif asset_type == 'dashboard':
                    analysis.affected_dashboards.append(node_id)
                elif asset_type == 'pipeline':
                    analysis.affected_pipelines.append(node_id)
            
            # Assess risk level
            analysis.risk_level = self._assess_risk_level(analysis, change_type)
            
            # Generate recommendations
            analysis.recommended_actions = self._generate_impact_recommendations(
                analysis, change_type, change_details
            )
            
            # Estimate fix time
            analysis.estimated_fix_time = self._estimate_fix_time(analysis, change_type)
            
            return analysis
            
        except Exception as e:
            self.logger.error(f"Impact analysis failed for {asset_id}: {e}")
            return ImpactAnalysis(target_asset_id=asset_id, change_type=change_type)

    def get_field_lineage(
        self,
        asset_id: str,
        field_name: str,
        direction: str = "both",
        max_depth: int = 3
    ) -> dict[str, Any]:
        """
        Get field-level lineage for a specific field
        
        Args:
            asset_id: Asset identifier
            field_name: Field name
            direction: Lineage direction
            max_depth: Maximum traversal depth
        
        Returns:
            Field lineage information
        """
        try:
            field_lineage = {
                'asset_id': asset_id,
                'field_name': field_name,
                'upstream_fields': [],
                'downstream_fields': [],
                'transformations': []
            }
            
            # Get edges involving this asset
            relevant_edges = [
                self.lineage_edges[edge_id] 
                for edge_id in self.asset_edges.get(asset_id, [])
                if edge_id in self.lineage_edges
            ]
            
            # Process field mappings
            for edge in relevant_edges:
                if not edge.field_mappings:
                    continue
                
                # Upstream: field is a target
                if edge.target_asset_id == asset_id:
                    for source_field, target_fields in edge.field_mappings.items():
                        if field_name in target_fields:
                            field_lineage['upstream_fields'].append({
                                'asset_id': edge.source_asset_id,
                                'field_name': source_field,
                                'transformation': edge.transformation_logic
                            })
                
                # Downstream: field is a source
                if edge.source_asset_id == asset_id:
                    target_fields = edge.field_mappings.get(field_name, [])
                    for target_field in target_fields:
                        field_lineage['downstream_fields'].append({
                            'asset_id': edge.target_asset_id,
                            'field_name': target_field,
                            'transformation': edge.transformation_logic
                        })
                
                # Record transformations
                if edge.transformation_logic and (
                    edge.source_asset_id == asset_id or edge.target_asset_id == asset_id
                ):
                    field_lineage['transformations'].append({
                        'edge_id': edge.edge_id,
                        'transformation': edge.transformation_logic,
                        'type': edge.lineage_type.value
                    })
            
            return field_lineage
            
        except Exception as e:
            self.logger.error(f"Field lineage retrieval failed: {e}")
            return {}

    def get_pipeline_lineage(self, pipeline_id: str) -> dict[str, Any]:
        """Get complete lineage for a pipeline"""
        try:
            with self._lock:
                pipeline_edges = [
                    self.lineage_edges[edge_id]
                    for edge_id in self.pipeline_edges.get(pipeline_id, [])
                    if edge_id in self.lineage_edges
                ]
                
                if not pipeline_edges:
                    return {'pipeline_id': pipeline_id, 'edges': [], 'assets': []}
                
                # Get all assets involved
                assets = set()
                for edge in pipeline_edges:
                    assets.add(edge.source_asset_id)
                    assets.add(edge.target_asset_id)
                
                # Group by transformation type
                transformations = defaultdict(list)
                for edge in pipeline_edges:
                    transformations[edge.lineage_type.value].append({
                        'source': edge.source_asset_id,
                        'target': edge.target_asset_id,
                        'logic': edge.transformation_logic
                    })
                
                return {
                    'pipeline_id': pipeline_id,
                    'total_edges': len(pipeline_edges),
                    'assets_involved': list(assets),
                    'transformations_by_type': dict(transformations),
                    'edges': [self._edge_to_dict(edge) for edge in pipeline_edges]
                }
                
        except Exception as e:
            self.logger.error(f"Pipeline lineage retrieval failed: {e}")
            return {}

    def validate_lineage_consistency(self) -> dict[str, Any]:
        """Validate lineage consistency and detect issues"""
        try:
            validation_results = {
                'total_edges': len(self.lineage_edges),
                'issues': [],
                'warnings': [],
                'statistics': {}
            }
            
            # Check for orphaned edges (missing assets)
            orphaned_edges = []
            for edge_id, edge in self.lineage_edges.items():
                # This would require integration with asset catalog
                # For now, just check basic consistency
                if not edge.source_asset_id or not edge.target_asset_id:
                    orphaned_edges.append(edge_id)
            
            if orphaned_edges:
                validation_results['issues'].append({
                    'type': 'orphaned_edges',
                    'count': len(orphaned_edges),
                    'details': orphaned_edges[:10]  # Show first 10
                })
            
            # Check for circular dependencies
            circular_deps = self._detect_circular_dependencies()
            if circular_deps:
                validation_results['issues'].append({
                    'type': 'circular_dependencies',
                    'count': len(circular_deps),
                    'details': circular_deps
                })
            
            # Check for duplicate edges
            edge_signatures = defaultdict(list)
            for edge_id, edge in self.lineage_edges.items():
                signature = f"{edge.source_asset_id}_{edge.target_asset_id}_{edge.lineage_type.value}"
                edge_signatures[signature].append(edge_id)
            
            duplicates = {sig: edges for sig, edges in edge_signatures.items() if len(edges) > 1}
            if duplicates:
                validation_results['warnings'].append({
                    'type': 'potential_duplicates',
                    'count': len(duplicates),
                    'details': {sig: edges[:5] for sig, edges in list(duplicates.items())[:5]}
                })
            
            # Calculate statistics
            lineage_types = defaultdict(int)
            capture_methods = defaultdict(int)
            
            for edge in self.lineage_edges.values():
                lineage_types[edge.lineage_type.value] += 1
                capture_methods[edge.capture_method.value] += 1
            
            validation_results['statistics'] = {
                'lineage_types': dict(lineage_types),
                'capture_methods': dict(capture_methods),
                'unique_assets': len(self.asset_edges),
                'unique_pipelines': len(self.pipeline_edges)
            }
            
            return validation_results
            
        except Exception as e:
            self.logger.error(f"Lineage validation failed: {e}")
            return {'error': str(e)}

    def _normalize_sql(self, sql: str) -> str:
        """Normalize SQL query for parsing"""
        # Remove comments
        sql = re.sub(r'--[^\n]*', '', sql)
        sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
        
        # Normalize whitespace
        sql = re.sub(r'\s+', ' ', sql)
        
        return sql.strip()

    def _extract_sql_relationships(self, sql: str) -> list[dict[str, Any]]:
        """Extract lineage relationships from SQL"""
        relationships = []
        
        try:
            # CREATE TABLE AS SELECT
            ctas_matches = self.sql_patterns['create_table_as'].findall(sql)
            for target, source in ctas_matches:
                relationships.append({
                    'source': source,
                    'target': target,
                    'type': LineageType.DERIVE,
                    'logic': 'CREATE TABLE AS SELECT'
                })
            
            # INSERT INTO
            insert_matches = self.sql_patterns['insert_into'].findall(sql)
            from_matches = self.sql_patterns['select_from'].findall(sql)
            
            for target in insert_matches:
                for source in from_matches:
                    relationships.append({
                        'source': source,
                        'target': target,
                        'type': LineageType.WRITE,
                        'logic': 'INSERT INTO ... SELECT FROM'
                    })
            
            # JOINs
            from_matches = self.sql_patterns['select_from'].findall(sql)
            join_matches = self.sql_patterns['join_tables'].findall(sql)
            
            if from_matches and join_matches:
                # Create JOIN relationship
                all_sources = from_matches + join_matches
                for i, source1 in enumerate(all_sources):
                    for source2 in all_sources[i+1:]:
                        relationships.append({
                            'source': source1,
                            'related_source': source2,
                            'type': LineageType.JOIN,
                            'logic': 'TABLE JOIN'
                        })
            
            # UNIONs
            union_matches = self.sql_patterns['union_with'].findall(sql)
            if len(union_matches) > 1:
                for source in union_matches:
                    relationships.append({
                        'source': source,
                        'type': LineageType.UNION,
                        'logic': 'UNION operation'
                    })
            
        except Exception as e:
            self.logger.error(f"SQL relationship extraction failed: {e}")
        
        return relationships

    def _traverse_lineage(
        self,
        asset_id: str,
        direction: str,
        max_depth: int,
        visited: set | None = None
    ) -> dict[str, Any]:
        """Traverse lineage graph using BFS"""
        if visited is None:
            visited = set()
        
        if max_depth <= 0 or asset_id in visited:
            return {'nodes': {}, 'edges': []}
        
        visited.add(asset_id)
        
        nodes = {asset_id: {'id': asset_id, 'depth': max_depth}}
        edges = []
        
        try:
            # Get edges for this asset
            asset_edge_ids = self.asset_edges.get(asset_id, [])
            
            for edge_id in asset_edge_ids:
                if edge_id not in self.lineage_edges:
                    continue
                
                edge = self.lineage_edges[edge_id]
                
                # Determine next asset based on direction
                if direction == "upstream":
                    if edge.target_asset_id == asset_id:
                        next_asset = edge.source_asset_id
                    else:
                        continue
                elif direction == "downstream":
                    if edge.source_asset_id == asset_id:
                        next_asset = edge.target_asset_id
                    else:
                        continue
                else:
                    continue
                
                edges.append(edge)
                
                # Recursive traversal
                if next_asset not in visited:
                    sub_result = self._traverse_lineage(
                        next_asset, direction, max_depth - 1, visited.copy()
                    )
                    
                    nodes.update(sub_result['nodes'])
                    edges.extend(sub_result['edges'])
        
        except Exception as e:
            self.logger.error(f"Lineage traversal failed: {e}")
        
        return {'nodes': nodes, 'edges': edges}

    def _find_critical_path(self, graph: LineageGraph) -> list[str]:
        """Find the critical path (longest path) in the lineage graph"""
        try:
            # Simple implementation - find longest path from root
            if not graph.edges:
                return [graph.root_asset_id]
            
            # Build adjacency list
            adj_list = defaultdict(list)
            for edge in graph.edges:
                adj_list[edge.source_asset_id].append(edge.target_asset_id)
            
            # DFS to find longest path
            def dfs(node: str, path: list[str], visited: set) -> list[str]:
                if node in visited:
                    return path
                
                visited.add(node)
                longest_path = path + [node]
                
                for neighbor in adj_list[node]:
                    candidate_path = dfs(neighbor, path + [node], visited.copy())
                    if len(candidate_path) > len(longest_path):
                        longest_path = candidate_path
                
                return longest_path
            
            return dfs(graph.root_asset_id, [], set())
        
        except Exception as e:
            self.logger.error(f"Critical path calculation failed: {e}")
            return [graph.root_asset_id]

    def _assess_risk_level(self, analysis: ImpactAnalysis, change_type: str) -> str:
        """Assess risk level based on impact analysis"""
        risk_factors = 0
        
        # Factor in number of affected assets
        total_affected = len(analysis.directly_affected) + len(analysis.indirectly_affected)
        if total_affected > 10:
            risk_factors += 2
        elif total_affected > 5:
            risk_factors += 1
        
        # Factor in affected business assets
        business_assets = (
            len(analysis.affected_reports) + 
            len(analysis.affected_dashboards) +
            len(analysis.affected_pipelines)
        )
        if business_assets > 5:
            risk_factors += 2
        elif business_assets > 2:
            risk_factors += 1
        
        # Factor in change type
        high_risk_changes = ['schema', 'location', 'format']
        if change_type in high_risk_changes:
            risk_factors += 2
        
        # Determine risk level
        if risk_factors >= 5:
            return "critical"
        elif risk_factors >= 3:
            return "high"
        elif risk_factors >= 1:
            return "medium"
        else:
            return "low"

    def _generate_impact_recommendations(
        self,
        analysis: ImpactAnalysis,
        change_type: str,
        change_details: dict[str, Any] | None
    ) -> list[str]:
        """Generate recommendations based on impact analysis"""
        recommendations = []
        
        if analysis.risk_level in ["high", "critical"]:
            recommendations.append("Perform thorough testing before implementing changes")
            recommendations.append("Coordinate with stakeholders of affected systems")
        
        if analysis.affected_reports:
            recommendations.append("Update report documentation and notify report users")
        
        if analysis.affected_dashboards:
            recommendations.append("Test dashboard functionality after changes")
        
        if analysis.affected_pipelines:
            recommendations.append("Update pipeline configurations and test data flow")
        
        if change_type == "schema":
            recommendations.append("Implement backward compatibility where possible")
            recommendations.append("Use schema evolution best practices")
        
        if len(analysis.directly_affected) > 5:
            recommendations.append("Consider phased rollout to minimize risk")
        
        return recommendations

    def _estimate_fix_time(self, analysis: ImpactAnalysis, change_type: str) -> int:
        """Estimate time to fix issues after change"""
        base_time = 2  # Base 2 hours
        
        # Factor in affected assets
        total_affected = len(analysis.directly_affected) + len(analysis.indirectly_affected)
        asset_time = total_affected * 0.5  # 30 minutes per affected asset
        
        # Factor in change complexity
        complexity_multiplier = {
            'schema': 2.0,
            'location': 1.5,
            'format': 2.0,
            'content': 1.0
        }.get(change_type, 1.0)
        
        estimated_hours = (base_time + asset_time) * complexity_multiplier
        return int(max(1, estimated_hours))

    def _detect_circular_dependencies(self) -> list[list[str]]:
        """Detect circular dependencies in lineage"""
        cycles = []
        
        try:
            # Build adjacency list
            adj_list = defaultdict(list)
            for edge in self.lineage_edges.values():
                adj_list[edge.source_asset_id].append(edge.target_asset_id)
            
            # DFS to detect cycles
            def dfs(node: str, path: list[str], visited: set) -> None:
                if node in path:
                    # Found cycle
                    cycle_start = path.index(node)
                    cycle = path[cycle_start:] + [node]
                    if cycle not in cycles:
                        cycles.append(cycle)
                    return
                
                if node in visited:
                    return
                
                visited.add(node)
                
                for neighbor in adj_list[node]:
                    dfs(neighbor, path + [node], visited.copy())
            
            # Check all nodes
            global_visited = set()
            for node in adj_list:
                if node not in global_visited:
                    dfs(node, [], set())
                    global_visited.add(node)
        
        except Exception as e:
            self.logger.error(f"Circular dependency detection failed: {e}")
        
        return cycles[:10]  # Limit to first 10 cycles

    def _persist_edge(self, edge: LineageEdge):
        """Persist lineage edge to storage"""
        try:
            edge_file = self.edges_path / f"{edge.edge_id}.json"
            with open(edge_file, 'w') as f:
                json.dump(self._edge_to_dict(edge), f, indent=2, default=str)
        except Exception as e:
            self.logger.error(f"Failed to persist lineage edge {edge.edge_id}: {e}")

    def _edge_to_dict(self, edge: LineageEdge) -> dict[str, Any]:
        """Convert edge to dictionary"""
        return {
            'edge_id': edge.edge_id,
            'source_asset_id': edge.source_asset_id,
            'target_asset_id': edge.target_asset_id,
            'lineage_type': edge.lineage_type.value,
            'transformation_logic': edge.transformation_logic,
            'field_mappings': edge.field_mappings,
            'pipeline_id': edge.pipeline_id,
            'job_id': edge.job_id,
            'execution_id': edge.execution_id,
            'capture_method': edge.capture_method.value,
            'confidence_score': edge.confidence_score,
            'created_at': edge.created_at.isoformat(),
            'created_by': edge.created_by,
            'query': edge.query,
            'script_path': edge.script_path,
            'function_name': edge.function_name,
            'data_quality_impact': edge.data_quality_impact,
            'business_impact': edge.business_impact,
            'tags': edge.tags,
            'metadata': edge.metadata
        }

    def _dict_to_edge(self, data: dict[str, Any]) -> LineageEdge:
        """Convert dictionary to edge"""
        return LineageEdge(
            edge_id=data['edge_id'],
            source_asset_id=data['source_asset_id'],
            target_asset_id=data['target_asset_id'],
            lineage_type=LineageType(data['lineage_type']),
            transformation_logic=data.get('transformation_logic'),
            field_mappings=data.get('field_mappings', {}),
            pipeline_id=data.get('pipeline_id'),
            job_id=data.get('job_id'),
            execution_id=data.get('execution_id'),
            capture_method=LineageCapture(data.get('capture_method', 'automatic')),
            confidence_score=data.get('confidence_score', 1.0),
            created_at=datetime.fromisoformat(data['created_at']),
            created_by=data.get('created_by'),
            query=data.get('query'),
            script_path=data.get('script_path'),
            function_name=data.get('function_name'),
            data_quality_impact=data.get('data_quality_impact', {}),
            business_impact=data.get('business_impact'),
            tags=data.get('tags', []),
            metadata=data.get('metadata', {})
        )