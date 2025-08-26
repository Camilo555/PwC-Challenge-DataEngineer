"""
Intelligent Data Lineage Tracking and Impact Analysis System
"""
from __future__ import annotations

import ast
import hashlib
import re
from collections import defaultdict, deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional, Set, Tuple, Union

import networkx as nx
import pandas as pd
from pydantic import BaseModel, Field
from sqlparse import parse as sql_parse
from sqlparse.sql import Statement
from sqlparse.tokens import Keyword, Name

from core.logging import get_logger


class LineageNodeType(str, Enum):
    """Types of nodes in data lineage graph"""
    SOURCE_TABLE = "source_table"
    TARGET_TABLE = "target_table"
    VIEW = "view"
    STORED_PROCEDURE = "stored_procedure"
    ETL_PROCESS = "etl_process"
    API_ENDPOINT = "api_endpoint"
    FILE_SYSTEM = "file_system"
    DATABASE = "database"
    COLUMN = "column"
    TRANSFORMATION = "transformation"


class ChangeType(str, Enum):
    """Types of changes that can trigger impact analysis"""
    SCHEMA_CHANGE = "schema_change"
    DATA_TYPE_CHANGE = "data_type_change"
    COLUMN_ADDED = "column_added"
    COLUMN_REMOVED = "column_removed"
    TABLE_RENAMED = "table_renamed"
    TRANSFORMATION_CHANGE = "transformation_change"
    BUSINESS_RULE_CHANGE = "business_rule_change"


class ImpactLevel(str, Enum):
    """Impact severity levels"""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


@dataclass
class LineageNode:
    """Node in the data lineage graph"""
    node_id: str
    node_type: LineageNodeType
    name: str
    schema_name: str = ""
    database_name: str = ""
    description: str = ""
    owner: str = ""
    tags: List[str] = field(default_factory=list)
    properties: Dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)
    
    def __post_init__(self):
        if not self.node_id:
            self.node_id = self._generate_node_id()
    
    def _generate_node_id(self) -> str:
        """Generate unique node ID"""
        identifier = f"{self.database_name}.{self.schema_name}.{self.name}"
        return hashlib.md5(identifier.encode()).hexdigest()[:12]


@dataclass
class LineageEdge:
    """Edge in the data lineage graph representing relationships"""
    source_node_id: str
    target_node_id: str
    relationship_type: str  # "feeds", "derives_from", "transforms", "references"
    transformation_logic: str = ""
    confidence_score: float = 1.0  # 0.0 to 1.0
    created_by: str = ""
    created_at: datetime = field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = field(default_factory=dict)


class ColumnLineage(BaseModel):
    """Column-level lineage information"""
    source_column: str
    target_column: str
    source_table: str
    target_table: str
    transformation_rule: str = ""
    data_type_source: str = ""
    data_type_target: str = ""
    business_rule: str = ""
    confidence_score: float = 1.0


class DataAssetMetadata(BaseModel):
    """Comprehensive metadata for data assets"""
    asset_id: str
    asset_name: str
    asset_type: LineageNodeType
    schema_definition: Dict[str, Any] = Field(default_factory=dict)
    sample_data: Optional[str] = None
    row_count: int = 0
    size_bytes: int = 0
    last_accessed: Optional[datetime] = None
    access_frequency: int = 0
    data_classification: str = "public"  # public, internal, confidential, restricted
    retention_policy: str = ""
    quality_score: float = 0.0
    business_criticality: str = "medium"  # low, medium, high, critical


class ImpactAnalysisResult(BaseModel):
    """Result of impact analysis"""
    change_id: str
    change_type: ChangeType
    affected_node: str
    impact_level: ImpactLevel
    affected_assets: List[str] = Field(default_factory=list)
    downstream_impacts: List[str] = Field(default_factory=list)
    upstream_dependencies: List[str] = Field(default_factory=list)
    estimated_effort_hours: float = 0.0
    recommendations: List[str] = Field(default_factory=list)
    risk_assessment: str = ""
    mitigation_steps: List[str] = Field(default_factory=list)
    stakeholders: List[str] = Field(default_factory=list)
    analysis_timestamp: datetime = Field(default_factory=datetime.utcnow)


class DataLineageTracker:
    """Intelligent data lineage tracking and impact analysis system"""
    
    def __init__(self):
        self.logger = get_logger(__name__)
        
        # Core lineage graph
        self.lineage_graph = nx.MultiDiGraph()
        
        # Storage for lineage data
        self.nodes: Dict[str, LineageNode] = {}
        self.edges: List[LineageEdge] = []
        self.column_lineage: List[ColumnLineage] = []
        self.asset_metadata: Dict[str, DataAssetMetadata] = {}
        
        # Impact analysis cache
        self.impact_cache: Dict[str, ImpactAnalysisResult] = {}
        self.dependency_cache: Dict[str, List[str]] = {}
        
        # Configuration
        self.max_traversal_depth = 10
        self.confidence_threshold = 0.7
        
        self.logger.info("DataLineageTracker initialized")
    
    def add_node(self, node: LineageNode) -> str:
        """Add a node to the lineage graph"""
        node_id = node.node_id
        self.nodes[node_id] = node
        
        # Add to NetworkX graph with attributes
        self.lineage_graph.add_node(node_id, **asdict(node))
        
        self.logger.debug(f"Added lineage node: {node.name} ({node_id})")
        return node_id
    
    def add_edge(self, edge: LineageEdge) -> bool:
        """Add an edge to the lineage graph"""
        if edge.source_node_id not in self.nodes or edge.target_node_id not in self.nodes:
            self.logger.warning(f"Cannot add edge: missing nodes {edge.source_node_id} -> {edge.target_node_id}")
            return False
        
        self.edges.append(edge)
        
        # Add to NetworkX graph with attributes
        self.lineage_graph.add_edge(
            edge.source_node_id,
            edge.target_node_id,
            **asdict(edge)
        )
        
        # Clear dependency cache as it may be outdated
        self.dependency_cache.clear()
        
        self.logger.debug(f"Added lineage edge: {edge.source_node_id} -> {edge.target_node_id}")
        return True
    
    def add_column_lineage(self, column_lineage: ColumnLineage):
        """Add column-level lineage information"""
        self.column_lineage.append(column_lineage)
        self.logger.debug(f"Added column lineage: {column_lineage.source_table}.{column_lineage.source_column} -> {column_lineage.target_table}.{column_lineage.target_column}")
    
    def register_asset_metadata(self, metadata: DataAssetMetadata):
        """Register comprehensive metadata for a data asset"""
        self.asset_metadata[metadata.asset_id] = metadata
        self.logger.debug(f"Registered metadata for asset: {metadata.asset_name}")
    
    def parse_sql_lineage(self, sql_query: str, query_context: Dict[str, Any] = None) -> List[LineageEdge]:
        """Parse SQL query to extract lineage relationships"""
        query_context = query_context or {}
        edges = []
        
        try:
            # Parse the SQL query
            parsed = sql_parse(sql_query)[0]
            
            # Extract table references
            source_tables = self._extract_source_tables(parsed)
            target_tables = self._extract_target_tables(parsed)
            
            # Create edges for each source -> target relationship
            for source in source_tables:
                for target in target_tables:
                    source_node_id = self._get_or_create_table_node(source, query_context)
                    target_node_id = self._get_or_create_table_node(target, query_context)
                    
                    edge = LineageEdge(
                        source_node_id=source_node_id,
                        target_node_id=target_node_id,
                        relationship_type="feeds",
                        transformation_logic=sql_query,
                        confidence_score=0.8,
                        created_by="sql_parser",
                        metadata={"query_type": self._get_query_type(parsed)}
                    )
                    edges.append(edge)
            
            # Extract column-level lineage
            column_mappings = self._extract_column_mappings(parsed, source_tables, target_tables)
            for mapping in column_mappings:
                self.add_column_lineage(mapping)
            
        except Exception as e:
            self.logger.error(f"Failed to parse SQL lineage: {str(e)}")
        
        return edges
    
    def parse_etl_lineage(self, etl_config: Dict[str, Any]) -> List[LineageEdge]:
        """Parse ETL configuration to extract lineage relationships"""
        edges = []
        
        try:
            # Extract ETL process information
            process_name = etl_config.get("name", "unknown_process")
            sources = etl_config.get("sources", [])
            targets = etl_config.get("targets", [])
            transformations = etl_config.get("transformations", [])
            
            # Create ETL process node
            etl_node = LineageNode(
                node_id="",
                node_type=LineageNodeType.ETL_PROCESS,
                name=process_name,
                description=etl_config.get("description", ""),
                properties={"transformations": transformations}
            )
            etl_node_id = self.add_node(etl_node)
            
            # Create edges from sources to ETL process
            for source in sources:
                source_node_id = self._get_or_create_data_source_node(source)
                edge = LineageEdge(
                    source_node_id=source_node_id,
                    target_node_id=etl_node_id,
                    relationship_type="feeds",
                    transformation_logic=str(transformations),
                    confidence_score=0.9,
                    created_by="etl_parser"
                )
                edges.append(edge)
            
            # Create edges from ETL process to targets
            for target in targets:
                target_node_id = self._get_or_create_data_source_node(target)
                edge = LineageEdge(
                    source_node_id=etl_node_id,
                    target_node_id=target_node_id,
                    relationship_type="transforms",
                    transformation_logic=str(transformations),
                    confidence_score=0.9,
                    created_by="etl_parser"
                )
                edges.append(edge)
            
        except Exception as e:
            self.logger.error(f"Failed to parse ETL lineage: {str(e)}")
        
        return edges
    
    def trace_upstream_lineage(self, node_id: str, max_depth: int = None) -> Dict[str, Any]:
        """Trace upstream lineage for a given node"""
        max_depth = max_depth or self.max_traversal_depth
        
        if node_id in self.dependency_cache:
            return {"cached": True, "upstream": self.dependency_cache[node_id]}
        
        upstream_nodes = []
        visited = set()
        queue = deque([(node_id, 0)])
        
        while queue:
            current_node, depth = queue.popleft()
            
            if depth >= max_depth or current_node in visited:
                continue
            
            visited.add(current_node)
            
            # Get predecessors (upstream nodes)
            predecessors = list(self.lineage_graph.predecessors(current_node))
            
            for pred in predecessors:
                if pred not in visited:
                    # Get edge data
                    edge_data = self.lineage_graph[pred][current_node]
                    if isinstance(edge_data, dict):
                        edge_data = list(edge_data.values())[0]  # Get first edge if multiple
                    
                    upstream_nodes.append({
                        "node_id": pred,
                        "node_info": self.nodes.get(pred, {}),
                        "relationship": edge_data.get("relationship_type", "unknown"),
                        "depth": depth + 1,
                        "confidence": edge_data.get("confidence_score", 0.0)
                    })
                    
                    queue.append((pred, depth + 1))
        
        # Cache result
        upstream_node_ids = [node["node_id"] for node in upstream_nodes]
        self.dependency_cache[node_id] = upstream_node_ids
        
        return {
            "upstream_nodes": upstream_nodes,
            "total_count": len(upstream_nodes),
            "max_depth_reached": max_depth,
            "analysis_timestamp": datetime.utcnow().isoformat()
        }
    
    def trace_downstream_lineage(self, node_id: str, max_depth: int = None) -> Dict[str, Any]:
        """Trace downstream lineage for a given node"""
        max_depth = max_depth or self.max_traversal_depth
        
        downstream_nodes = []
        visited = set()
        queue = deque([(node_id, 0)])
        
        while queue:
            current_node, depth = queue.popleft()
            
            if depth >= max_depth or current_node in visited:
                continue
            
            visited.add(current_node)
            
            # Get successors (downstream nodes)
            successors = list(self.lineage_graph.successors(current_node))
            
            for succ in successors:
                if succ not in visited:
                    # Get edge data
                    edge_data = self.lineage_graph[current_node][succ]
                    if isinstance(edge_data, dict):
                        edge_data = list(edge_data.values())[0]  # Get first edge if multiple
                    
                    downstream_nodes.append({
                        "node_id": succ,
                        "node_info": self.nodes.get(succ, {}),
                        "relationship": edge_data.get("relationship_type", "unknown"),
                        "depth": depth + 1,
                        "confidence": edge_data.get("confidence_score", 0.0)
                    })
                    
                    queue.append((succ, depth + 1))
        
        return {
            "downstream_nodes": downstream_nodes,
            "total_count": len(downstream_nodes),
            "max_depth_reached": max_depth,
            "analysis_timestamp": datetime.utcnow().isoformat()
        }
    
    def analyze_change_impact(self, change_description: Dict[str, Any]) -> ImpactAnalysisResult:
        """Analyze the impact of a proposed change"""
        
        change_id = change_description.get("change_id", f"change_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}")
        affected_node_id = change_description.get("node_id", "")
        change_type = ChangeType(change_description.get("change_type", "schema_change"))
        
        # Check cache first
        cache_key = f"{change_id}_{affected_node_id}_{change_type.value}"
        if cache_key in self.impact_cache:
            return self.impact_cache[cache_key]
        
        # Perform impact analysis
        downstream_analysis = self.trace_downstream_lineage(affected_node_id)
        upstream_analysis = self.trace_upstream_lineage(affected_node_id)
        
        # Calculate impact level
        impact_level = self._calculate_impact_level(change_type, downstream_analysis, upstream_analysis)
        
        # Extract affected assets
        affected_assets = [node["node_id"] for node in downstream_analysis["downstream_nodes"]]
        downstream_impacts = [node["node_id"] for node in downstream_analysis["downstream_nodes"] if node["depth"] <= 3]
        upstream_dependencies = [node["node_id"] for node in upstream_analysis["upstream_nodes"]]
        
        # Generate recommendations and mitigation steps
        recommendations = self._generate_change_recommendations(change_type, impact_level, affected_assets)
        mitigation_steps = self._generate_mitigation_steps(change_type, impact_level)
        risk_assessment = self._assess_change_risk(change_type, impact_level, len(affected_assets))
        
        # Estimate effort
        estimated_effort = self._estimate_change_effort(change_type, impact_level, len(affected_assets))
        
        # Identify stakeholders
        stakeholders = self._identify_stakeholders(affected_assets)
        
        result = ImpactAnalysisResult(
            change_id=change_id,
            change_type=change_type,
            affected_node=affected_node_id,
            impact_level=impact_level,
            affected_assets=affected_assets,
            downstream_impacts=downstream_impacts,
            upstream_dependencies=upstream_dependencies,
            estimated_effort_hours=estimated_effort,
            recommendations=recommendations,
            risk_assessment=risk_assessment,
            mitigation_steps=mitigation_steps,
            stakeholders=stakeholders
        )
        
        # Cache result
        self.impact_cache[cache_key] = result
        
        self.logger.info(f"Impact analysis completed for change {change_id}: {impact_level.value} impact on {len(affected_assets)} assets")
        return result
    
    def get_column_level_lineage(self, table_name: str, column_name: str) -> List[Dict[str, Any]]:
        """Get column-level lineage for a specific column"""
        column_lineage_data = []
        
        for lineage in self.column_lineage:
            if (lineage.target_table == table_name and lineage.target_column == column_name) or \
               (lineage.source_table == table_name and lineage.source_column == column_name):
                column_lineage_data.append({
                    "source_column": lineage.source_column,
                    "target_column": lineage.target_column,
                    "source_table": lineage.source_table,
                    "target_table": lineage.target_table,
                    "transformation_rule": lineage.transformation_rule,
                    "business_rule": lineage.business_rule,
                    "confidence_score": lineage.confidence_score
                })
        
        return column_lineage_data
    
    def generate_lineage_report(self, node_id: str = None) -> Dict[str, Any]:
        """Generate comprehensive lineage report"""
        
        report = {
            "report_timestamp": datetime.utcnow().isoformat(),
            "total_nodes": len(self.nodes),
            "total_edges": len(self.edges),
            "node_type_distribution": self._get_node_type_distribution(),
            "graph_metrics": self._calculate_graph_metrics()
        }
        
        if node_id:
            # Specific node report
            if node_id in self.nodes:
                upstream = self.trace_upstream_lineage(node_id)
                downstream = self.trace_downstream_lineage(node_id)
                
                report.update({
                    "focus_node": asdict(self.nodes[node_id]),
                    "upstream_lineage": upstream,
                    "downstream_lineage": downstream,
                    "column_lineage": self.get_column_level_lineage(self.nodes[node_id].name, ""),
                    "asset_metadata": self.asset_metadata.get(node_id, {})
                })
        else:
            # Global report
            report.update({
                "top_connected_nodes": self._get_top_connected_nodes(),
                "orphaned_nodes": self._get_orphaned_nodes(),
                "data_classification_summary": self._get_data_classification_summary()
            })
        
        return report
    
    def export_lineage_graph(self, format_type: str = "json", file_path: str = None) -> Union[str, Dict[str, Any]]:
        """Export lineage graph in various formats"""
        
        if format_type == "json":
            graph_data = {
                "nodes": [asdict(node) for node in self.nodes.values()],
                "edges": [asdict(edge) for edge in self.edges],
                "metadata": {
                    "export_timestamp": datetime.utcnow().isoformat(),
                    "total_nodes": len(self.nodes),
                    "total_edges": len(self.edges)
                }
            }
            
            if file_path:
                import json
                with open(file_path, 'w') as f:
                    json.dump(graph_data, f, indent=2, default=str)
                return file_path
            
            return graph_data
        
        elif format_type == "dot":
            # Export as DOT format for Graphviz visualization
            dot_content = "digraph lineage {\n"
            dot_content += "  rankdir=LR;\n"
            dot_content += "  node [shape=box];\n\n"
            
            # Add nodes
            for node in self.nodes.values():
                dot_content += f'  "{node.node_id}" [label="{node.name}\\n({node.node_type.value})"];\n'
            
            dot_content += "\n"
            
            # Add edges
            for edge in self.edges:
                dot_content += f'  "{edge.source_node_id}" -> "{edge.target_node_id}" [label="{edge.relationship_type}"];\n'
            
            dot_content += "}\n"
            
            if file_path:
                with open(file_path, 'w') as f:
                    f.write(dot_content)
                return file_path
            
            return dot_content
        
        else:
            raise ValueError(f"Unsupported export format: {format_type}")
    
    def _extract_source_tables(self, parsed_sql: Statement) -> List[str]:
        """Extract source table names from parsed SQL"""
        source_tables = []
        
        def extract_from_tokens(tokens):
            in_from = False
            for token in tokens:
                if token.ttype is Keyword and token.value.upper() == 'FROM':
                    in_from = True
                    continue
                elif token.ttype is Keyword and token.value.upper() in ['WHERE', 'GROUP', 'ORDER', 'HAVING']:
                    in_from = False
                elif in_from and token.ttype is Name:
                    source_tables.append(token.value)
                
                if hasattr(token, 'tokens'):
                    extract_from_tokens(token.tokens)
        
        extract_from_tokens(parsed_sql.tokens)
        return source_tables
    
    def _extract_target_tables(self, parsed_sql: Statement) -> List[str]:
        """Extract target table names from parsed SQL"""
        target_tables = []
        
        # Look for INSERT, UPDATE, CREATE patterns
        sql_text = str(parsed_sql).upper()
        
        # INSERT INTO pattern
        insert_match = re.search(r'INSERT\s+INTO\s+(\w+)', sql_text)
        if insert_match:
            target_tables.append(insert_match.group(1))
        
        # UPDATE pattern
        update_match = re.search(r'UPDATE\s+(\w+)', sql_text)
        if update_match:
            target_tables.append(update_match.group(1))
        
        # CREATE TABLE pattern
        create_match = re.search(r'CREATE\s+TABLE\s+(\w+)', sql_text)
        if create_match:
            target_tables.append(create_match.group(1))
        
        return target_tables
    
    def _extract_column_mappings(self, parsed_sql: Statement, source_tables: List[str], target_tables: List[str]) -> List[ColumnLineage]:
        """Extract column-level lineage from SQL"""
        # This is a simplified implementation
        # A production version would need more sophisticated SQL parsing
        column_mappings = []
        
        # Basic pattern matching for SELECT columns
        sql_text = str(parsed_sql)
        select_pattern = r'SELECT\s+(.*?)\s+FROM'
        select_match = re.search(select_pattern, sql_text, re.IGNORECASE | re.DOTALL)
        
        if select_match:
            columns_text = select_match.group(1)
            columns = [col.strip() for col in columns_text.split(',')]
            
            for i, column in enumerate(columns):
                if len(source_tables) > 0 and len(target_tables) > 0:
                    mapping = ColumnLineage(
                        source_column=column,
                        target_column=column,  # Simplified assumption
                        source_table=source_tables[0],
                        target_table=target_tables[0],
                        transformation_rule="direct_mapping",
                        confidence_score=0.7
                    )
                    column_mappings.append(mapping)
        
        return column_mappings
    
    def _get_query_type(self, parsed_sql: Statement) -> str:
        """Determine the type of SQL query"""
        sql_text = str(parsed_sql).strip().upper()
        
        if sql_text.startswith('SELECT'):
            return 'SELECT'
        elif sql_text.startswith('INSERT'):
            return 'INSERT'
        elif sql_text.startswith('UPDATE'):
            return 'UPDATE'
        elif sql_text.startswith('DELETE'):
            return 'DELETE'
        elif sql_text.startswith('CREATE'):
            return 'CREATE'
        elif sql_text.startswith('ALTER'):
            return 'ALTER'
        elif sql_text.startswith('DROP'):
            return 'DROP'
        else:
            return 'UNKNOWN'
    
    def _get_or_create_table_node(self, table_name: str, context: Dict[str, Any]) -> str:
        """Get existing table node or create new one"""
        # Generate node ID based on table name and context
        schema = context.get("schema", "default")
        database = context.get("database", "default")
        
        node_id_base = f"{database}.{schema}.{table_name}"
        node_id = hashlib.md5(node_id_base.encode()).hexdigest()[:12]
        
        if node_id not in self.nodes:
            node = LineageNode(
                node_id=node_id,
                node_type=LineageNodeType.SOURCE_TABLE,
                name=table_name,
                schema_name=schema,
                database_name=database,
                description=f"Auto-discovered table: {table_name}"
            )
            self.add_node(node)
        
        return node_id
    
    def _get_or_create_data_source_node(self, source_config: Dict[str, Any]) -> str:
        """Get existing data source node or create new one"""
        source_name = source_config.get("name", "unknown_source")
        source_type = source_config.get("type", "table")
        
        # Map source type to node type
        node_type_map = {
            "table": LineageNodeType.SOURCE_TABLE,
            "file": LineageNodeType.FILE_SYSTEM,
            "api": LineageNodeType.API_ENDPOINT,
            "view": LineageNodeType.VIEW
        }
        
        node_type = node_type_map.get(source_type, LineageNodeType.SOURCE_TABLE)
        
        node_id_base = f"{source_config.get('database', 'default')}.{source_config.get('schema', 'default')}.{source_name}"
        node_id = hashlib.md5(node_id_base.encode()).hexdigest()[:12]
        
        if node_id not in self.nodes:
            node = LineageNode(
                node_id=node_id,
                node_type=node_type,
                name=source_name,
                schema_name=source_config.get("schema", ""),
                database_name=source_config.get("database", ""),
                description=source_config.get("description", ""),
                properties=source_config
            )
            self.add_node(node)
        
        return node_id
    
    def _calculate_impact_level(self, change_type: ChangeType, downstream: Dict[str, Any], upstream: Dict[str, Any]) -> ImpactLevel:
        """Calculate the impact level of a change"""
        downstream_count = downstream.get("total_count", 0)
        upstream_count = upstream.get("total_count", 0)
        
        # Impact scoring based on change type and affected nodes
        impact_score = 0
        
        # Base score by change type
        change_scores = {
            ChangeType.SCHEMA_CHANGE: 8,
            ChangeType.DATA_TYPE_CHANGE: 7,
            ChangeType.COLUMN_REMOVED: 9,
            ChangeType.COLUMN_ADDED: 3,
            ChangeType.TABLE_RENAMED: 6,
            ChangeType.TRANSFORMATION_CHANGE: 5,
            ChangeType.BUSINESS_RULE_CHANGE: 4
        }
        
        impact_score += change_scores.get(change_type, 5)
        
        # Adjust based on downstream impact
        if downstream_count > 20:
            impact_score += 5
        elif downstream_count > 10:
            impact_score += 3
        elif downstream_count > 5:
            impact_score += 2
        
        # Adjust based on upstream dependencies
        if upstream_count > 10:
            impact_score += 2
        elif upstream_count > 5:
            impact_score += 1
        
        # Convert score to impact level
        if impact_score >= 12:
            return ImpactLevel.CRITICAL
        elif impact_score >= 8:
            return ImpactLevel.HIGH
        elif impact_score >= 5:
            return ImpactLevel.MEDIUM
        else:
            return ImpactLevel.LOW
    
    def _generate_change_recommendations(self, change_type: ChangeType, impact_level: ImpactLevel, affected_assets: List[str]) -> List[str]:
        """Generate recommendations based on change analysis"""
        recommendations = []
        
        if impact_level in [ImpactLevel.HIGH, ImpactLevel.CRITICAL]:
            recommendations.extend([
                "Plan change during maintenance window",
                "Coordinate with all downstream system owners",
                "Prepare rollback plan",
                "Conduct thorough impact testing"
            ])
        
        if change_type == ChangeType.SCHEMA_CHANGE:
            recommendations.extend([
                "Update all dependent ETL processes",
                "Validate data type compatibility",
                "Update documentation and data dictionary"
            ])
        
        if change_type == ChangeType.COLUMN_REMOVED:
            recommendations.extend([
                "Identify and update all queries referencing removed columns",
                "Consider gradual deprecation instead of immediate removal",
                "Update application code and reports"
            ])
        
        if len(affected_assets) > 10:
            recommendations.append("Consider phased implementation to reduce risk")
        
        return recommendations
    
    def _generate_mitigation_steps(self, change_type: ChangeType, impact_level: ImpactLevel) -> List[str]:
        """Generate mitigation steps for the change"""
        mitigation_steps = []
        
        # Common mitigation steps
        mitigation_steps.extend([
            "Create backup of current state",
            "Test changes in development environment",
            "Validate data quality post-change"
        ])
        
        if impact_level in [ImpactLevel.HIGH, ImpactLevel.CRITICAL]:
            mitigation_steps.extend([
                "Implement gradual rollout",
                "Set up monitoring and alerting",
                "Prepare emergency rollback procedure",
                "Schedule post-implementation review"
            ])
        
        if change_type in [ChangeType.SCHEMA_CHANGE, ChangeType.DATA_TYPE_CHANGE]:
            mitigation_steps.extend([
                "Validate data migration scripts",
                "Test backward compatibility",
                "Update data validation rules"
            ])
        
        return mitigation_steps
    
    def _assess_change_risk(self, change_type: ChangeType, impact_level: ImpactLevel, affected_count: int) -> str:
        """Assess the risk level of the proposed change"""
        risk_factors = []
        
        if impact_level == ImpactLevel.CRITICAL:
            risk_factors.append("Critical impact level")
        
        if affected_count > 20:
            risk_factors.append("High number of affected systems")
        
        if change_type in [ChangeType.COLUMN_REMOVED, ChangeType.SCHEMA_CHANGE]:
            risk_factors.append("Breaking change type")
        
        if not risk_factors:
            return "Low risk - minimal impact expected"
        elif len(risk_factors) == 1:
            return f"Medium risk - {risk_factors[0]}"
        else:
            return f"High risk - Multiple factors: {', '.join(risk_factors)}"
    
    def _estimate_change_effort(self, change_type: ChangeType, impact_level: ImpactLevel, affected_count: int) -> float:
        """Estimate effort in hours for implementing the change"""
        base_hours = {
            ChangeType.SCHEMA_CHANGE: 8,
            ChangeType.DATA_TYPE_CHANGE: 6,
            ChangeType.COLUMN_ADDED: 2,
            ChangeType.COLUMN_REMOVED: 10,
            ChangeType.TABLE_RENAMED: 4,
            ChangeType.TRANSFORMATION_CHANGE: 6,
            ChangeType.BUSINESS_RULE_CHANGE: 4
        }
        
        effort = base_hours.get(change_type, 4)
        
        # Multiply by impact level
        impact_multipliers = {
            ImpactLevel.LOW: 1,
            ImpactLevel.MEDIUM: 1.5,
            ImpactLevel.HIGH: 2.5,
            ImpactLevel.CRITICAL: 4
        }
        
        effort *= impact_multipliers[impact_level]
        
        # Add effort based on affected systems (1 hour per 2 systems)
        effort += (affected_count / 2)
        
        return round(effort, 1)
    
    def _identify_stakeholders(self, affected_assets: List[str]) -> List[str]:
        """Identify stakeholders based on affected assets"""
        stakeholders = set()
        
        for asset_id in affected_assets:
            if asset_id in self.nodes:
                node = self.nodes[asset_id]
                if node.owner:
                    stakeholders.add(node.owner)
        
        # Add default stakeholders for high-impact changes
        if len(affected_assets) > 10:
            stakeholders.update(["data_team", "platform_team", "business_intelligence"])
        
        return list(stakeholders)
    
    def _get_node_type_distribution(self) -> Dict[str, int]:
        """Get distribution of node types"""
        distribution = defaultdict(int)
        for node in self.nodes.values():
            distribution[node.node_type.value] += 1
        return dict(distribution)
    
    def _calculate_graph_metrics(self) -> Dict[str, Any]:
        """Calculate graph-level metrics"""
        if not self.lineage_graph.nodes():
            return {}
        
        metrics = {
            "density": nx.density(self.lineage_graph),
            "is_connected": nx.is_weakly_connected(self.lineage_graph),
            "number_of_components": nx.number_weakly_connected_components(self.lineage_graph),
            "average_in_degree": sum(dict(self.lineage_graph.in_degree()).values()) / len(self.lineage_graph.nodes()),
            "average_out_degree": sum(dict(self.lineage_graph.out_degree()).values()) / len(self.lineage_graph.nodes())
        }
        
        return metrics
    
    def _get_top_connected_nodes(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get nodes with highest connectivity"""
        node_connectivity = []
        
        for node_id in self.lineage_graph.nodes():
            in_degree = self.lineage_graph.in_degree(node_id)
            out_degree = self.lineage_graph.out_degree(node_id)
            total_degree = in_degree + out_degree
            
            if node_id in self.nodes:
                node_connectivity.append({
                    "node_id": node_id,
                    "name": self.nodes[node_id].name,
                    "type": self.nodes[node_id].node_type.value,
                    "in_degree": in_degree,
                    "out_degree": out_degree,
                    "total_degree": total_degree
                })
        
        # Sort by total degree and return top nodes
        node_connectivity.sort(key=lambda x: x["total_degree"], reverse=True)
        return node_connectivity[:limit]
    
    def _get_orphaned_nodes(self) -> List[Dict[str, Any]]:
        """Get nodes with no connections"""
        orphaned = []
        
        for node_id in self.lineage_graph.nodes():
            if self.lineage_graph.degree(node_id) == 0 and node_id in self.nodes:
                orphaned.append({
                    "node_id": node_id,
                    "name": self.nodes[node_id].name,
                    "type": self.nodes[node_id].node_type.value,
                    "created_at": self.nodes[node_id].created_at.isoformat()
                })
        
        return orphaned
    
    def _get_data_classification_summary(self) -> Dict[str, int]:
        """Get summary of data classification across assets"""
        classification_counts = defaultdict(int)
        
        for metadata in self.asset_metadata.values():
            classification_counts[metadata.data_classification] += 1
        
        return dict(classification_counts)


# Factory function
def create_lineage_tracker() -> DataLineageTracker:
    """Create DataLineageTracker instance"""
    return DataLineageTracker()


# Example usage and testing
if __name__ == "__main__":
    # Initialize lineage tracker
    tracker = create_lineage_tracker()
    
    try:
        # Example: Create sample nodes
        source_table = LineageNode(
            node_id="",
            node_type=LineageNodeType.SOURCE_TABLE,
            name="raw_transactions",
            schema_name="bronze",
            database_name="retail_dw",
            description="Raw transaction data from POS systems"
        )
        
        target_table = LineageNode(
            node_id="",
            node_type=LineageNodeType.TARGET_TABLE,
            name="fact_sales",
            schema_name="gold",
            database_name="retail_dw",
            description="Processed sales fact table"
        )
        
        # Add nodes
        source_id = tracker.add_node(source_table)
        target_id = tracker.add_node(target_table)
        
        # Add edge
        edge = LineageEdge(
            source_node_id=source_id,
            target_node_id=target_id,
            relationship_type="transforms",
            transformation_logic="Aggregate transactions by date and product",
            confidence_score=0.95
        )
        
        tracker.add_edge(edge)
        
        # Add sample metadata
        metadata = DataAssetMetadata(
            asset_id=target_id,
            asset_name="fact_sales",
            asset_type=LineageNodeType.TARGET_TABLE,
            row_count=1000000,
            quality_score=0.98,
            business_criticality="high"
        )
        
        tracker.register_asset_metadata(metadata)
        
        # Test lineage tracing
        print("Testing upstream lineage...")
        upstream = tracker.trace_upstream_lineage(target_id)
        print(f"Found {upstream['total_count']} upstream dependencies")
        
        print("Testing downstream lineage...")
        downstream = tracker.trace_downstream_lineage(source_id)
        print(f"Found {downstream['total_count']} downstream consumers")
        
        # Test impact analysis
        print("Testing impact analysis...")
        change_desc = {
            "change_id": "CHG001",
            "node_id": source_id,
            "change_type": "schema_change",
            "description": "Add new customer_segment column"
        }
        
        impact = tracker.analyze_change_impact(change_desc)
        print(f"Impact analysis: {impact.impact_level.value} level impact")
        print(f"Recommendations: {len(impact.recommendations)}")
        
        # Test SQL parsing
        print("Testing SQL lineage parsing...")
        sql_query = """
        INSERT INTO gold.fact_sales 
        SELECT t.transaction_id, t.customer_id, t.amount, t.transaction_date
        FROM bronze.raw_transactions t
        WHERE t.transaction_date >= '2024-01-01'
        """
        
        sql_edges = tracker.parse_sql_lineage(sql_query, {"schema": "retail", "database": "retail_dw"})
        print(f"Extracted {len(sql_edges)} edges from SQL")
        
        # Generate report
        print("Generating lineage report...")
        report = tracker.generate_lineage_report()
        print(f"Report includes {report['total_nodes']} nodes and {report['total_edges']} edges")
        
        # Export graph
        print("Exporting lineage graph...")
        graph_json = tracker.export_lineage_graph("json")
        print(f"Exported graph with {len(graph_json['nodes'])} nodes")
        
        print("✅ Data Lineage Tracker testing completed successfully!")
        
    except Exception as e:
        print(f"❌ Testing failed: {str(e)}")
        import traceback
        traceback.print_exc()