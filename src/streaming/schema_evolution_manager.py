"""
Schema Evolution Manager for Real-time Streaming Data
Provides comprehensive schema management, evolution, and compatibility handling for streaming pipelines
"""
from __future__ import annotations

import json
import uuid
import hashlib
from abc import ABC, abstractmethod
from dataclasses import dataclass, field, asdict
from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

from delta import DeltaTable
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import (
    col, current_timestamp, expr, lit, struct, when, coalesce, 
    split, regexp_extract, length, size, array, map_keys, map_values
)
from pyspark.sql.types import (
    StructType, StructField, DataType, StringType, IntegerType, 
    LongType, DoubleType, FloatType, BooleanType, TimestampType, 
    DateType, ArrayType, MapType, DecimalType, NullType
)

from core.config.unified_config import get_unified_config
from core.logging import get_logger
from etl.spark.delta_lake_manager import DeltaLakeManager
from monitoring.advanced_metrics import get_metrics_collector
from src.streaming.kafka_manager import KafkaManager, StreamingTopic
from src.streaming.hybrid_messaging_architecture import (
    HybridMessagingArchitecture, RabbitMQManager, RabbitMQConfig,
    HybridMessage, MessageType, MessagePriority
)
from src.streaming.event_sourcing_cache_integration import (
    EventCache, CacheConfig, CacheStrategy, ConsistencyLevel
)


class CompatibilityType(Enum):
    """Schema compatibility types"""
    BACKWARD = "backward"        # New schema can read old data
    FORWARD = "forward"          # Old schema can read new data
    FULL = "full"               # Both backward and forward
    NONE = "none"               # No compatibility required
    TRANSITIVE_BACKWARD = "transitive_backward"
    TRANSITIVE_FORWARD = "transitive_forward"
    TRANSITIVE_FULL = "transitive_full"


class EvolutionType(Enum):
    """Types of schema evolution"""
    ADD_COLUMN = "add_column"
    DROP_COLUMN = "drop_column"
    RENAME_COLUMN = "rename_column"
    CHANGE_TYPE = "change_type"
    MODIFY_NULLABLE = "modify_nullable"
    RESTRUCTURE = "restructure"
    MERGE_TABLES = "merge_tables"


class SchemaChangeImpact(Enum):
    """Impact level of schema changes"""
    LOW = "low"           # No breaking changes
    MEDIUM = "medium"     # Some compatibility concerns
    HIGH = "high"         # Breaking changes
    CRITICAL = "critical" # Major restructuring required


@dataclass
class SchemaVersion:
    """Schema version information"""
    version_id: str
    schema_name: str
    schema_hash: str
    schema_definition: StructType
    created_at: datetime
    compatibility_type: CompatibilityType
    metadata: Dict[str, Any] = field(default_factory=dict)
    is_active: bool = True


@dataclass
class SchemaChange:
    """Individual schema change record"""
    change_id: str
    change_type: EvolutionType
    field_name: str
    old_definition: Optional[Dict[str, Any]] = None
    new_definition: Optional[Dict[str, Any]] = None
    impact_level: SchemaChangeImpact = SchemaChangeImpact.LOW
    description: str = ""
    timestamp: datetime = field(default_factory=datetime.now)


@dataclass
class SchemaEvolutionPlan:
    """Schema evolution execution plan"""
    plan_id: str
    source_version: str
    target_version: str
    changes: List[SchemaChange]
    compatibility_checks: List[str]
    migration_steps: List[str]
    rollback_plan: List[str]
    estimated_duration: timedelta
    created_at: datetime = field(default_factory=datetime.now)


@dataclass
class SchemaRegistry:
    """Schema registry for version management"""
    registry_name: str
    schemas: Dict[str, SchemaVersion] = field(default_factory=dict)
    version_history: List[str] = field(default_factory=list)
    active_version: Optional[str] = None
    compatibility_mode: CompatibilityType = CompatibilityType.BACKWARD


class SchemaCompatibilityChecker:
    """Checks schema compatibility between versions"""
    
    def __init__(self):
        self.logger = get_logger(__name__)
        
    def check_compatibility(
        self, 
        old_schema: StructType, 
        new_schema: StructType, 
        compatibility_type: CompatibilityType
    ) -> Tuple[bool, List[str]]:
        """Check if schemas are compatible"""
        try:
            issues = []
            
            if compatibility_type == CompatibilityType.BACKWARD:
                issues.extend(self._check_backward_compatibility(old_schema, new_schema))
            elif compatibility_type == CompatibilityType.FORWARD:
                issues.extend(self._check_forward_compatibility(old_schema, new_schema))
            elif compatibility_type == CompatibilityType.FULL:
                issues.extend(self._check_backward_compatibility(old_schema, new_schema))
                issues.extend(self._check_forward_compatibility(old_schema, new_schema))
            elif compatibility_type == CompatibilityType.NONE:
                # No compatibility checks needed
                pass
            
            is_compatible = len(issues) == 0
            return is_compatible, issues
            
        except Exception as e:
            self.logger.error(f"Compatibility check failed: {e}")
            return False, [f"Compatibility check error: {str(e)}"]
    
    def _check_backward_compatibility(self, old_schema: StructType, new_schema: StructType) -> List[str]:
        """Check backward compatibility (new schema can read old data)"""
        issues = []
        
        old_fields = {f.name: f for f in old_schema.fields}
        new_fields = {f.name: f for f in new_schema.fields}
        
        # Check for removed fields
        for field_name in old_fields:
            if field_name not in new_fields:
                issues.append(f"Field '{field_name}' was removed (breaks backward compatibility)")
        
        # Check for type changes in existing fields
        for field_name in old_fields:
            if field_name in new_fields:
                old_field = old_fields[field_name]
                new_field = new_fields[field_name]
                
                # Check if types are compatible
                if not self._are_types_compatible(old_field.dataType, new_field.dataType):
                    issues.append(f"Field '{field_name}' type changed from {old_field.dataType} to {new_field.dataType}")
                
                # Check nullability
                if old_field.nullable and not new_field.nullable:
                    issues.append(f"Field '{field_name}' changed from nullable to non-nullable")
        
        return issues
    
    def _check_forward_compatibility(self, old_schema: StructType, new_schema: StructType) -> List[str]:
        """Check forward compatibility (old schema can read new data)"""
        issues = []
        
        old_fields = {f.name: f for f in old_schema.fields}
        new_fields = {f.name: f for f in new_schema.fields}
        
        # Check for added non-nullable fields
        for field_name in new_fields:
            if field_name not in old_fields:
                new_field = new_fields[field_name]
                if not new_field.nullable:
                    issues.append(f"Added non-nullable field '{field_name}' (breaks forward compatibility)")
        
        return issues
    
    def _are_types_compatible(self, old_type: DataType, new_type: DataType) -> bool:
        """Check if data types are compatible"""
        # Same types are always compatible
        if old_type == new_type:
            return True
        
        # Define compatible type mappings
        compatible_types = {
            IntegerType(): [LongType(), DoubleType(), FloatType()],
            LongType(): [DoubleType(), FloatType()],
            FloatType(): [DoubleType()],
            StringType(): [],  # String can only go to string
            BooleanType(): [StringType()],  # Boolean can go to string
        }
        
        for old_base_type, compatible_list in compatible_types.items():
            if type(old_type) == type(old_base_type):
                return any(type(new_type) == type(compat_type) for compat_type in compatible_list)
        
        return False


class SchemaEvolutionEngine:
    """Engine for managing schema evolution"""
    
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.logger = get_logger(__name__)
        self.compatibility_checker = SchemaCompatibilityChecker()
        
    def create_evolution_plan(
        self,
        current_schema: StructType,
        target_schema: StructType,
        compatibility_type: CompatibilityType
    ) -> SchemaEvolutionPlan:
        """Create schema evolution plan"""
        try:
            plan_id = f"evolution_plan_{uuid.uuid4().hex[:8]}"
            
            # Analyze differences
            changes = self._analyze_schema_differences(current_schema, target_schema)
            
            # Check compatibility
            is_compatible, issues = self.compatibility_checker.check_compatibility(
                current_schema, target_schema, compatibility_type
            )
            
            # Generate migration steps
            migration_steps = self._generate_migration_steps(changes)
            
            # Generate rollback plan
            rollback_plan = self._generate_rollback_plan(changes)
            
            # Estimate duration
            estimated_duration = self._estimate_migration_duration(changes)
            
            return SchemaEvolutionPlan(
                plan_id=plan_id,
                source_version=self._calculate_schema_hash(current_schema),
                target_version=self._calculate_schema_hash(target_schema),
                changes=changes,
                compatibility_checks=issues,
                migration_steps=migration_steps,
                rollback_plan=rollback_plan,
                estimated_duration=estimated_duration
            )
            
        except Exception as e:
            self.logger.error(f"Failed to create evolution plan: {e}")
            raise
    
    def _analyze_schema_differences(
        self, 
        current_schema: StructType, 
        target_schema: StructType
    ) -> List[SchemaChange]:
        """Analyze differences between schemas"""
        changes = []
        
        current_fields = {f.name: f for f in current_schema.fields}
        target_fields = {f.name: f for f in target_schema.fields}
        
        # Check for new fields
        for field_name in target_fields:
            if field_name not in current_fields:
                changes.append(SchemaChange(
                    change_id=f"add_{field_name}_{uuid.uuid4().hex[:8]}",
                    change_type=EvolutionType.ADD_COLUMN,
                    field_name=field_name,
                    new_definition=self._field_to_dict(target_fields[field_name]),
                    impact_level=SchemaChangeImpact.LOW,
                    description=f"Add new field '{field_name}'"
                ))
        
        # Check for removed fields
        for field_name in current_fields:
            if field_name not in target_fields:
                changes.append(SchemaChange(
                    change_id=f"drop_{field_name}_{uuid.uuid4().hex[:8]}",
                    change_type=EvolutionType.DROP_COLUMN,
                    field_name=field_name,
                    old_definition=self._field_to_dict(current_fields[field_name]),
                    impact_level=SchemaChangeImpact.HIGH,
                    description=f"Remove field '{field_name}'"
                ))
        
        # Check for modified fields
        for field_name in current_fields:
            if field_name in target_fields:
                current_field = current_fields[field_name]
                target_field = target_fields[field_name]
                
                if current_field.dataType != target_field.dataType:
                    changes.append(SchemaChange(
                        change_id=f"type_{field_name}_{uuid.uuid4().hex[:8]}",
                        change_type=EvolutionType.CHANGE_TYPE,
                        field_name=field_name,
                        old_definition=self._field_to_dict(current_field),
                        new_definition=self._field_to_dict(target_field),
                        impact_level=SchemaChangeImpact.MEDIUM,
                        description=f"Change type of '{field_name}' from {current_field.dataType} to {target_field.dataType}"
                    ))
                
                if current_field.nullable != target_field.nullable:
                    changes.append(SchemaChange(
                        change_id=f"nullable_{field_name}_{uuid.uuid4().hex[:8]}",
                        change_type=EvolutionType.MODIFY_NULLABLE,
                        field_name=field_name,
                        old_definition=self._field_to_dict(current_field),
                        new_definition=self._field_to_dict(target_field),
                        impact_level=SchemaChangeImpact.MEDIUM,
                        description=f"Change nullability of '{field_name}'"
                    ))
        
        return changes
    
    def _field_to_dict(self, field: StructField) -> Dict[str, Any]:
        """Convert StructField to dictionary"""
        return {
            "name": field.name,
            "type": str(field.dataType),
            "nullable": field.nullable,
            "metadata": dict(field.metadata) if field.metadata else {}
        }
    
    def _generate_migration_steps(self, changes: List[SchemaChange]) -> List[str]:
        """Generate migration steps from changes"""
        steps = []
        
        # Sort changes by impact (low impact first)
        sorted_changes = sorted(changes, key=lambda x: x.impact_level.value)
        
        for change in sorted_changes:
            if change.change_type == EvolutionType.ADD_COLUMN:
                steps.append(f"ADD COLUMN {change.field_name} {change.new_definition['type']} {'NULL' if change.new_definition['nullable'] else 'NOT NULL'}")
            elif change.change_type == EvolutionType.DROP_COLUMN:
                steps.append(f"DROP COLUMN {change.field_name}")
            elif change.change_type == EvolutionType.CHANGE_TYPE:
                steps.append(f"ALTER COLUMN {change.field_name} TYPE {change.new_definition['type']}")
            elif change.change_type == EvolutionType.MODIFY_NULLABLE:
                null_constraint = "DROP NOT NULL" if change.new_definition['nullable'] else "SET NOT NULL"
                steps.append(f"ALTER COLUMN {change.field_name} {null_constraint}")
        
        return steps
    
    def _generate_rollback_plan(self, changes: List[SchemaChange]) -> List[str]:
        """Generate rollback plan"""
        rollback_steps = []
        
        # Reverse the changes
        for change in reversed(changes):
            if change.change_type == EvolutionType.ADD_COLUMN:
                rollback_steps.append(f"DROP COLUMN {change.field_name}")
            elif change.change_type == EvolutionType.DROP_COLUMN:
                rollback_steps.append(f"ADD COLUMN {change.field_name} {change.old_definition['type']}")
            elif change.change_type == EvolutionType.CHANGE_TYPE:
                rollback_steps.append(f"ALTER COLUMN {change.field_name} TYPE {change.old_definition['type']}")
        
        return rollback_steps
    
    def _estimate_migration_duration(self, changes: List[SchemaChange]) -> timedelta:
        """Estimate migration duration"""
        base_time = timedelta(minutes=5)  # Base time for any migration
        
        for change in changes:
            if change.impact_level == SchemaChangeImpact.LOW:
                base_time += timedelta(minutes=2)
            elif change.impact_level == SchemaChangeImpact.MEDIUM:
                base_time += timedelta(minutes=5)
            elif change.impact_level == SchemaChangeImpact.HIGH:
                base_time += timedelta(minutes=15)
            elif change.impact_level == SchemaChangeImpact.CRITICAL:
                base_time += timedelta(minutes=30)
        
        return base_time
    
    def _calculate_schema_hash(self, schema: StructType) -> str:
        """Calculate hash for schema"""
        schema_str = str(schema)
        return hashlib.md5(schema_str.encode()).hexdigest()


class StreamingSchemaManager:
    """Manager for streaming schema evolution with distributed caching"""
    
    def __init__(self, spark: SparkSession, registry_path: str = "./schema_registry", enable_distributed_caching: bool = True, redis_url: str = "redis://localhost:6379", enable_messaging: bool = True):
        self.spark = spark
        self.logger = get_logger(__name__)
        self.registry_path = Path(registry_path)
        self.registry_path.mkdir(parents=True, exist_ok=True)
        
        # Enhanced infrastructure components
        self.cache_manager: Optional[EventCache] = None
        self.messaging_manager: Optional[HybridMessagingArchitecture] = None
        self.rabbitmq_manager: Optional[RabbitMQManager] = None
        
        # Initialize enhanced infrastructure
        if enable_distributed_caching:
            self._initialize_cache_manager(redis_url)
        if enable_messaging:
            self._initialize_messaging_manager()
        
        # Components
        self.evolution_engine = SchemaEvolutionEngine(spark)
        self.kafka_manager = KafkaManager()
        self.metrics_collector = get_metrics_collector()
        
        # Schema registries with caching
        self.registries: Dict[str, SchemaRegistry] = {}
        self.cached_schemas: Dict[str, str] = {}  # schema_name -> cache_key mapping
        
        # Load existing registries
        self._load_registries()
        
        self.logger.info("Streaming Schema Manager initialized with distributed caching")
    
    def _initialize_cache_manager(self, redis_url: str):
        """Initialize distributed cache manager for schema data"""
        try:
            cache_config = CacheConfig(
                redis_url=redis_url,
                default_ttl=3600,  # 1 hour for schema data
                cache_strategy=CacheStrategy.CACHE_ASIDE,
                consistency_level=ConsistencyLevel.STRONG,  # Strong consistency for schema data
                enable_cache_warming=True
            )
            self.cache_manager = EventCache(cache_config)
            self.logger.info("Schema cache manager initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize schema cache manager: {e}")
    
    def _initialize_messaging_manager(self):
        """Initialize messaging for schema change notifications"""
        try:
            rabbitmq_config = RabbitMQConfig(
                host="localhost",
                port=5672,
                enable_dead_letter=True
            )
            self.rabbitmq_manager = RabbitMQManager(rabbitmq_config)
            self.messaging_manager = HybridMessagingArchitecture(
                kafka_manager=self.kafka_manager,
                rabbitmq_manager=self.rabbitmq_manager,
                cache_manager=self.cache_manager
            )
            self.logger.info("Schema messaging manager initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize schema messaging manager: {e}")
        
    def _cache_schema_registry(self, registry_name: str, registry_data: Dict[str, Any]):
        """Cache schema registry data"""
        try:
            if self.cache_manager:
                cache_key = f"schema_registry:{registry_name}"
                self.cache_manager.set(
                    cache_key,
                    json.dumps(registry_data),
                    ttl=7200  # 2 hours for registry data
                )
                self.cached_schemas[registry_name] = cache_key
                self.logger.debug(f"Cached schema registry: {registry_name}")
        except Exception as e:
            self.logger.warning(f"Failed to cache schema registry {registry_name}: {e}")
    
    def _get_cached_schema_registry(self, registry_name: str) -> Optional[Dict[str, Any]]:
        """Get cached schema registry data"""
        try:
            if self.cache_manager and registry_name in self.cached_schemas:
                cache_key = self.cached_schemas[registry_name]
                cached_data = self.cache_manager.get(cache_key)
                if cached_data:
                    return json.loads(cached_data)
        except Exception as e:
            self.logger.warning(f"Failed to get cached schema registry {registry_name}: {e}")
        return None
    
    def _invalidate_schema_cache(self, registry_name: str, version_id: str = None):
        """Invalidate cached schema data"""
        try:
            if self.cache_manager:
                # Invalidate registry cache
                registry_cache_key = f"schema_registry:{registry_name}"
                # In a real implementation, you'd use Redis pattern deletion
                self.logger.debug(f"Invalidating schema cache for registry: {registry_name}")
                
                # Send cache invalidation event
                if self.messaging_manager:
                    message = HybridMessage(
                        message_id=str(uuid.uuid4()),
                        message_type=MessageType.EVENT,
                        routing_key="schema.cache.invalidate",
                        payload={
                            "registry_name": registry_name,
                            "version_id": version_id,
                            "timestamp": datetime.now().isoformat()
                        },
                        priority=MessagePriority.HIGH
                    )
                    self.messaging_manager.send_event(message)
        except Exception as e:
            self.logger.warning(f"Failed to invalidate schema cache: {e}")
    
    def _load_registries(self):
        """Load existing schema registries with caching support"""
        try:
            registry_files = list(self.registry_path.glob("*.json"))
            
            for registry_file in registry_files:
                with open(registry_file, 'r') as f:
                    registry_data = json.load(f)
                
                registry_name = registry_file.stem
                registry = SchemaRegistry(
                    registry_name=registry_name,
                    compatibility_mode=CompatibilityType(registry_data.get("compatibility_mode", "backward"))
                )
                
                # Load schema versions
                for version_data in registry_data.get("schemas", []):
                    schema_json = version_data["schema_definition"]
                    schema = StructType.fromJson(schema_json)
                    
                    version = SchemaVersion(
                        version_id=version_data["version_id"],
                        schema_name=registry_name,
                        schema_hash=version_data["schema_hash"],
                        schema_definition=schema,
                        created_at=datetime.fromisoformat(version_data["created_at"]),
                        compatibility_type=CompatibilityType(version_data["compatibility_type"]),
                        metadata=version_data.get("metadata", {}),
                        is_active=version_data.get("is_active", True)
                    )
                    
                    registry.schemas[version.version_id] = version
                    registry.version_history.append(version.version_id)
                
                registry.active_version = registry_data.get("active_version")
                self.registries[registry_name] = registry
            
            self.logger.info(f"Loaded {len(self.registries)} schema registries")
            
            # Cache loaded registries
            for registry_name, registry in self.registries.items():
                registry_data = {
                    "registry_name": registry.registry_name,
                    "active_version": registry.active_version,
                    "compatibility_mode": registry.compatibility_mode.value,
                    "total_versions": len(registry.schemas),
                    "loaded_at": datetime.now().isoformat()
                }
                self._cache_schema_registry(registry_name, registry_data)
            
        except Exception as e:
            self.logger.error(f"Failed to load registries: {e}")
    
    def _save_registry(self, registry_name: str):
        """Save schema registry to disk"""
        try:
            registry = self.registries[registry_name]
            registry_file = self.registry_path / f"{registry_name}.json"
            
            registry_data = {
                "registry_name": registry.registry_name,
                "active_version": registry.active_version,
                "compatibility_mode": registry.compatibility_mode.value,
                "schemas": []
            }
            
            for version in registry.schemas.values():
                version_data = {
                    "version_id": version.version_id,
                    "schema_hash": version.schema_hash,
                    "schema_definition": version.schema_definition.jsonValue(),
                    "created_at": version.created_at.isoformat(),
                    "compatibility_type": version.compatibility_type.value,
                    "metadata": version.metadata,
                    "is_active": version.is_active
                }
                registry_data["schemas"].append(version_data)
            
            with open(registry_file, 'w') as f:
                json.dump(registry_data, f, indent=2)
            
            self.logger.debug(f"Saved registry: {registry_name}")
            
        except Exception as e:
            self.logger.error(f"Failed to save registry {registry_name}: {e}")
    
    def register_schema(
        self,
        schema_name: str,
        schema: StructType,
        compatibility_type: CompatibilityType = CompatibilityType.BACKWARD,
        metadata: Dict[str, Any] = None,
        use_cache: bool = True
    ) -> str:
        """Register a new schema version with caching support"""
        try:
            # Create registry if it doesn't exist
            if schema_name not in self.registries:
                self.registries[schema_name] = SchemaRegistry(
                    registry_name=schema_name,
                    compatibility_mode=compatibility_type
                )
            
            registry = self.registries[schema_name]
            
            # Calculate schema hash
            schema_hash = self.evolution_engine._calculate_schema_hash(schema)
            
            # Check if this schema already exists (with cache check first)
            existing_version = None
            if use_cache and self.cache_manager:
                cached_version = self.cache_manager.get(f"schema_hash:{schema_hash}")
                if cached_version:
                    existing_version = json.loads(cached_version).get("version_id")
                    self.logger.debug(f"Found cached schema version: {existing_version}")
            
            if not existing_version:
                existing_version = self._find_version_by_hash(schema_name, schema_hash)
            
            if existing_version:
                self.logger.info(f"Schema version already exists: {existing_version}")
                return existing_version
            
            # Check compatibility with active version
            if registry.active_version:
                active_schema = registry.schemas[registry.active_version].schema_definition
                is_compatible, issues = self.evolution_engine.compatibility_checker.check_compatibility(
                    active_schema, schema, compatibility_type
                )
                
                if not is_compatible:
                    self.logger.warning(f"Schema compatibility issues: {issues}")
                    # You might want to raise an exception here or handle differently
            
            # Create new version
            version_id = f"v{len(registry.schemas) + 1}_{uuid.uuid4().hex[:8]}"
            
            version = SchemaVersion(
                version_id=version_id,
                schema_name=schema_name,
                schema_hash=schema_hash,
                schema_definition=schema,
                created_at=datetime.now(),
                compatibility_type=compatibility_type,
                metadata=metadata or {}
            )
            
            # Add to registry
            registry.schemas[version_id] = version
            registry.version_history.append(version_id)
            registry.active_version = version_id
            
            # Save registry
            self._save_registry(schema_name)
            
            # Cache the new schema version
            if self.cache_manager:
                schema_cache_data = {
                    "version_id": version_id,
                    "schema_name": schema_name,
                    "schema_hash": schema_hash,
                    "created_at": datetime.now().isoformat()
                }
                self.cache_manager.set(
                    f"schema_hash:{schema_hash}",
                    json.dumps(schema_cache_data),
                    ttl=3600  # 1 hour
                )
                
                # Cache schema definition for fast retrieval
                self.cache_manager.set(
                    f"schema_definition:{schema_name}:{version_id}",
                    json.dumps(schema.jsonValue()),
                    ttl=7200  # 2 hours
                )
            
            # Invalidate related caches
            self._invalidate_schema_cache(schema_name, version_id)
            
            # Send enhanced notification
            self._notify_schema_registration(schema_name, version_id, {
                "cached": self.cache_manager is not None,
                "compatibility_type": compatibility_type.value,
                "field_count": len(schema.fields),
                "metadata": metadata
            })
            
            self.logger.info(f"Registered new schema version: {schema_name} v{version_id}")
            return version_id
            
        except Exception as e:
            self.logger.error(f"Failed to register schema: {e}")
            raise
    
    def _find_version_by_hash(self, schema_name: str, schema_hash: str) -> Optional[str]:
        """Find schema version by hash"""
        if schema_name not in self.registries:
            return None
        
        registry = self.registries[schema_name]
        for version_id, version in registry.schemas.items():
            if version.schema_hash == schema_hash:
                return version_id
        
        return None
    
    def evolve_schema(
        self,
        schema_name: str,
        new_schema: StructType,
        compatibility_type: CompatibilityType = None
    ) -> SchemaEvolutionPlan:
        """Evolve schema with compatibility checking"""
        try:
            if schema_name not in self.registries:
                raise ValueError(f"Schema registry '{schema_name}' not found")
            
            registry = self.registries[schema_name]
            
            if not registry.active_version:
                # No active version, just register
                version_id = self.register_schema(schema_name, new_schema, compatibility_type or registry.compatibility_mode)
                return SchemaEvolutionPlan(
                    plan_id=f"initial_{uuid.uuid4().hex[:8]}",
                    source_version="none",
                    target_version=version_id,
                    changes=[],
                    compatibility_checks=[],
                    migration_steps=[],
                    rollback_plan=[],
                    estimated_duration=timedelta(minutes=1)
                )
            
            # Get current active schema
            current_version = registry.schemas[registry.active_version]
            current_schema = current_version.schema_definition
            
            # Create evolution plan
            evolution_plan = self.evolution_engine.create_evolution_plan(
                current_schema,
                new_schema,
                compatibility_type or registry.compatibility_mode
            )
            
            # Execute evolution if compatible
            if not evolution_plan.compatibility_checks:
                new_version_id = self.register_schema(
                    schema_name, 
                    new_schema, 
                    compatibility_type or registry.compatibility_mode
                )
                evolution_plan.target_version = new_version_id
            
            return evolution_plan
            
        except Exception as e:
            self.logger.error(f"Schema evolution failed: {e}")
            raise
    
    def get_schema(self, schema_name: str, version_id: str = None, use_cache: bool = True) -> Optional[StructType]:
        """Get schema by name and version with caching support"""
        try:
            # Try cache first
            if use_cache and self.cache_manager:
                if version_id is None:
                    # Get active version from cache
                    cached_registry = self._get_cached_schema_registry(schema_name)
                    if cached_registry:
                        version_id = cached_registry.get("active_version")
                
                if version_id:
                    cache_key = f"schema_definition:{schema_name}:{version_id}"
                    cached_schema = self.cache_manager.get(cache_key)
                    if cached_schema:
                        schema_json = json.loads(cached_schema)
                        self.logger.debug(f"Using cached schema for {schema_name}:{version_id}")
                        return StructType.fromJson(schema_json)
            
            if schema_name not in self.registries:
                return None
            
            registry = self.registries[schema_name]
            
            if version_id is None:
                version_id = registry.active_version
            
            if version_id and version_id in registry.schemas:
                schema = registry.schemas[version_id].schema_definition
                
                # Cache the schema definition for faster future access
                if self.cache_manager:
                    self.cache_manager.set(
                        f"schema_definition:{schema_name}:{version_id}",
                        json.dumps(schema.jsonValue()),
                        ttl=7200  # 2 hours
                    )
                
                return schema
            
            return None
            
        except Exception as e:
            self.logger.error(f"Failed to get schema: {e}")
            return None
    
    def detect_schema_drift(self, schema_name: str, actual_schema: StructType) -> List[str]:
        """Detect schema drift between registered and actual schemas"""
        try:
            registered_schema = self.get_schema(schema_name)
            if not registered_schema:
                return [f"No registered schema found for '{schema_name}'"]
            
            # Compare schemas
            _, issues = self.evolution_engine.compatibility_checker.check_compatibility(
                registered_schema, actual_schema, CompatibilityType.FULL
            )
            
            return issues
            
        except Exception as e:
            self.logger.error(f"Schema drift detection failed: {e}")
            return [f"Drift detection error: {str(e)}"]
    
    def auto_evolve_from_data(
        self, 
        schema_name: str, 
        df: DataFrame,
        confidence_threshold: float = 0.8
    ) -> bool:
        """Automatically evolve schema based on incoming data"""
        try:
            current_schema = self.get_schema(schema_name)
            if not current_schema:
                # Register initial schema
                self.register_schema(schema_name, df.schema)
                return True
            
            # Detect differences
            drift_issues = self.detect_schema_drift(schema_name, df.schema)
            
            if not drift_issues:
                return True  # No changes needed
            
            # Analyze if evolution is safe
            evolution_plan = self.evolution_engine.create_evolution_plan(
                current_schema, df.schema, CompatibilityType.BACKWARD
            )
            
            # Calculate confidence score
            high_impact_changes = sum(1 for change in evolution_plan.changes 
                                    if change.impact_level in [SchemaChangeImpact.HIGH, SchemaChangeImpact.CRITICAL])
            
            confidence = 1.0 - (high_impact_changes * 0.3)
            
            if confidence >= confidence_threshold:
                # Safe to auto-evolve
                self.register_schema(schema_name, df.schema)
                self.logger.info(f"Auto-evolved schema '{schema_name}' with confidence {confidence:.2f}")
                return True
            else:
                self.logger.warning(f"Schema evolution requires manual approval (confidence: {confidence:.2f})")
                return False
                
        except Exception as e:
            self.logger.error(f"Auto-evolution failed: {e}")
            return False
    
    def _notify_schema_registration(self, schema_name: str, version_id: str, additional_data: Dict[str, Any] = None):
        """Send enhanced schema registration notification"""
        try:
            notification = {
                "event_type": "schema_registration",
                "schema_name": schema_name,
                "version_id": version_id,
                "timestamp": datetime.now().isoformat(),
                "registry_path": str(self.registry_path),
                "cached": self.cache_manager is not None,
                "additional_data": additional_data or {}
            }
            
            # Send via Kafka
            self.kafka_manager.produce_message(
                StreamingTopic.SYSTEM_EVENTS,
                notification,
                key=f"schema_{schema_name}"
            )
            
            # Also send via RabbitMQ for immediate processing
            if self.messaging_manager:
                message = HybridMessage(
                    message_id=str(uuid.uuid4()),
                    message_type=MessageType.EVENT,
                    routing_key=f"schema.registration.{schema_name}",
                    payload=notification,
                    priority=MessagePriority.HIGH
                )
                self.messaging_manager.send_event(message)
            
        except Exception as e:
            self.logger.warning(f"Failed to send schema notification: {e}")
    
    def get_cache_metrics(self) -> Dict[str, Any]:
        """Get schema cache performance metrics"""
        cache_metrics = {}
        if self.cache_manager:
            cache_metrics = self.cache_manager.metrics.get_metrics_summary()
        return cache_metrics
    
    def get_registry_status(self, schema_name: str = None) -> Dict[str, Any]:
        """Get status of schema registries with caching info"""
        try:
            if schema_name:
                if schema_name not in self.registries:
                    return {"error": f"Registry '{schema_name}' not found"}
                
                registry = self.registries[schema_name]
                status = {
                    "registry_name": registry.registry_name,
                    "active_version": registry.active_version,
                    "total_versions": len(registry.schemas),
                    "compatibility_mode": registry.compatibility_mode.value,
                    "version_history": registry.version_history,
                    "cached": self.cache_manager is not None,
                    "cache_metrics": self.get_cache_metrics(),
                    "schemas": {
                        v_id: {
                            "version_id": version.version_id,
                            "schema_hash": version.schema_hash,
                            "created_at": version.created_at.isoformat(),
                            "is_active": version.is_active,
                            "field_count": len(version.schema_definition.fields),
                            "cached": self.cache_manager.get(f"schema_definition:{schema_name}:{v_id}") is not None if self.cache_manager else False
                        }
                        for v_id, version in registry.schemas.items()
                    }
                }
                return status
            else:
                return {
                    "total_registries": len(self.registries),
                    "cache_enabled": self.cache_manager is not None,
                    "messaging_enabled": self.messaging_manager is not None,
                    "cache_metrics": self.get_cache_metrics(),
                    "registries": {
                        name: {
                            "active_version": registry.active_version,
                            "total_versions": len(registry.schemas),
                            "compatibility_mode": registry.compatibility_mode.value,
                            "cached": name in self.cached_schemas
                        }
                        for name, registry in self.registries.items()
                    }
                }
                
        except Exception as e:
            self.logger.error(f"Failed to get registry status: {e}")
            return {"error": str(e)}


# Factory functions
def create_schema_evolution_manager(
    spark: SparkSession,
    registry_path: str = "./schema_registry"
) -> StreamingSchemaManager:
    """Create schema evolution manager instance"""
    return StreamingSchemaManager(spark, registry_path)


# Example usage
if __name__ == "__main__":
    from pyspark.sql import SparkSession
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
    
    # Create Spark session
    spark = (
        SparkSession.builder
        .appName("SchemaEvolutionManager")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )
    
    try:
        print("Testing Schema Evolution Manager...")
        
        # Create schema manager
        schema_manager = create_schema_evolution_manager(spark, "./test_schema_registry")
        
        # Define test schemas
        schema_v1 = StructType([
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), True),
            StructField("amount", DoubleType(), True)
        ])
        
        schema_v2 = StructType([
            StructField("id", IntegerType(), False),
            StructField("name", StringType(), True),
            StructField("amount", DoubleType(), True),
            StructField("category", StringType(), True)  # New field
        ])
        
        # Register initial schema
        version_1 = schema_manager.register_schema("test_table", schema_v1)
        print(f"✅ Registered schema v1: {version_1}")
        
        # Evolve schema
        evolution_plan = schema_manager.evolve_schema("test_table", schema_v2)
        print(f"✅ Schema evolution plan created with {len(evolution_plan.changes)} changes")
        
        # Test schema drift detection
        drift_issues = schema_manager.detect_schema_drift("test_table", schema_v1)
        print(f"✅ Schema drift detection: {len(drift_issues)} issues found")
        
        # Get registry status
        status = schema_manager.get_registry_status("test_table")
        print(f"✅ Registry status: {status['total_versions']} versions")
        
        print("✅ Schema Evolution Manager testing completed")
        
    except Exception as e:
        print(f"❌ Testing failed: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()