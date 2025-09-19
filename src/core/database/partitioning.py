"""
Advanced Database Partitioning Manager
=====================================

Comprehensive partitioning strategies for enterprise-scale data warehouse:
- Time-based partitioning for fact tables
- Hash partitioning for large dimension tables
- Range partitioning for analytical workloads
- Hybrid partitioning strategies
- Automatic partition maintenance and pruning
- Performance optimization through partition elimination
"""
from __future__ import annotations

import asyncio
import hashlib
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta, date
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from uuid import UUID

from sqlalchemy import text, inspect, MetaData, Table, Column
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from sqlalchemy.pool import NullPool
from sqlalchemy.exc import SQLAlchemyError

from core.logging import get_logger

logger = get_logger(__name__)


class PartitionStrategy(str, Enum):
    """Partition strategy types."""
    RANGE = "range"
    HASH = "hash"
    LIST = "list"
    HYBRID = "hybrid"
    TIME_SERIES = "time_series"


class PartitionInterval(str, Enum):
    """Time-based partition intervals."""
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"
    QUARTERLY = "quarterly"
    YEARLY = "yearly"


class PartitionState(str, Enum):
    """Partition states."""
    ACTIVE = "active"
    ARCHIVED = "archived"
    DROPPED = "dropped"
    MAINTENANCE = "maintenance"


@dataclass
class PartitionConfig:
    """Configuration for table partitioning."""
    table_name: str
    strategy: PartitionStrategy
    partition_key: str
    interval: Optional[PartitionInterval] = None
    retention_periods: int = 24  # Number of partitions to keep
    auto_create_partitions: bool = True
    auto_drop_old_partitions: bool = True
    partition_prefix: str = ""
    hash_partitions: int = 16  # For hash partitioning
    compression: bool = True
    indexes: List[str] = field(default_factory=list)
    constraints: List[str] = field(default_factory=list)
    statistics_target: int = 100


@dataclass 
class PartitionInfo:
    """Information about a database partition."""
    name: str
    parent_table: str
    strategy: PartitionStrategy
    bounds: Dict[str, Any]
    state: PartitionState
    size_bytes: int = 0
    row_count: int = 0
    last_analyzed: Optional[datetime] = None
    created_at: Optional[datetime] = None
    data_range: Optional[Tuple[Any, Any]] = None


class DatabasePartitioningManager:
    """
    Advanced database partitioning manager for enterprise-scale performance.
    
    Features:
    - Automatic partition creation and maintenance
    - Multiple partitioning strategies (range, hash, list, hybrid)
    - Time-series optimized partitioning for fact tables
    - Partition pruning and elimination optimization
    - Statistics and performance monitoring
    - Automated archival and cleanup
    """

    def __init__(self, engine: AsyncEngine):
        self.engine = engine
        self.partition_configs: Dict[str, PartitionConfig] = {}
        self.partition_registry: Dict[str, List[PartitionInfo]] = {}
        self.maintenance_history: List[Dict[str, Any]] = []
        
        # Performance tracking
        self.metrics = {
            "partitions_created": 0,
            "partitions_dropped": 0,
            "maintenance_runs": 0,
            "query_acceleration_pct": 0.0,
            "storage_saved_bytes": 0,
            "last_maintenance": None
        }

    def register_partition_config(self, config: PartitionConfig):
        """Register a partition configuration."""
        self.partition_configs[config.table_name] = config
        logger.info(f"Registered partition config for table: {config.table_name}")

    def get_default_partition_configs(self) -> List[PartitionConfig]:
        """Get default partition configurations for retail data warehouse."""
        return [
            # FACT_SALE - Time-series partitioning by date_key (monthly)
            PartitionConfig(
                table_name="fact_sale",
                strategy=PartitionStrategy.TIME_SERIES,
                partition_key="date_key",
                interval=PartitionInterval.MONTHLY,
                retention_periods=36,  # 3 years
                auto_create_partitions=True,
                auto_drop_old_partitions=True,
                partition_prefix="fact_sale_",
                compression=True,
                indexes=[
                    "customer_key, date_key",
                    "product_key, date_key", 
                    "country_key, date_key",
                    "total_amount, date_key",
                    "created_at"
                ],
                constraints=[
                    "CHECK (total_amount >= 0)",
                    "CHECK (quantity > 0)"
                ],
                statistics_target=1000
            ),

            # FACT_SALE - Hash partitioning for parallel processing
            PartitionConfig(
                table_name="fact_sale_hash",
                strategy=PartitionStrategy.HASH,
                partition_key="customer_key",
                hash_partitions=32,  # 32 hash partitions for parallel processing
                retention_periods=0,  # Keep all hash partitions
                auto_create_partitions=True,
                auto_drop_old_partitions=False,
                partition_prefix="fact_sale_hash_",
                compression=True,
                indexes=[
                    "customer_key",
                    "date_key, customer_key",
                    "total_amount"
                ]
            ),

            # ETL_PROCESSING_LOG - Time-series partitioning (daily)
            PartitionConfig(
                table_name="etl_processing_log",
                strategy=PartitionStrategy.TIME_SERIES,
                partition_key="processed_at",
                interval=PartitionInterval.DAILY,
                retention_periods=90,  # 90 days
                auto_create_partitions=True,
                auto_drop_old_partitions=True,
                partition_prefix="etl_log_",
                compression=True,
                indexes=["processed_at", "pipeline_name", "status"]
            ),

            # CUSTOMER_EVENTS - Time-series partitioning (weekly) 
            PartitionConfig(
                table_name="customer_events",
                strategy=PartitionStrategy.TIME_SERIES,
                partition_key="event_timestamp",
                interval=PartitionInterval.WEEKLY,
                retention_periods=52,  # 1 year
                auto_create_partitions=True,
                auto_drop_old_partitions=True,
                partition_prefix="customer_events_",
                compression=True,
                indexes=[
                    "customer_id, event_timestamp",
                    "event_type, event_timestamp",
                    "event_timestamp"
                ]
            ),

            # API_LOGS - Time-series partitioning (daily)
            PartitionConfig(
                table_name="api_logs", 
                strategy=PartitionStrategy.TIME_SERIES,
                partition_key="timestamp",
                interval=PartitionInterval.DAILY,
                retention_periods=30,  # 30 days
                auto_create_partitions=True,
                auto_drop_old_partitions=True,
                partition_prefix="api_logs_",
                compression=True,
                indexes=[
                    "timestamp",
                    "endpoint, timestamp", 
                    "status_code, timestamp"
                ]
            ),

            # DIM_CUSTOMER - Hash partitioning for large dimension
            PartitionConfig(
                table_name="dim_customer_partitioned",
                strategy=PartitionStrategy.HASH,
                partition_key="customer_key",
                hash_partitions=16,
                retention_periods=0,
                auto_create_partitions=True,
                auto_drop_old_partitions=False,
                partition_prefix="dim_customer_",
                compression=True,
                indexes=[
                    "customer_key",
                    "customer_id, is_current",
                    "customer_segment",
                    "rfm_segment"
                ]
            ),

            # SALES_TRANSACTIONS - Hybrid partitioning (time + hash)
            PartitionConfig(
                table_name="sales_transactions_hybrid",
                strategy=PartitionStrategy.HYBRID,
                partition_key="created_at, customer_id",
                interval=PartitionInterval.MONTHLY,
                hash_partitions=8,  # Sub-partitions within each time partition
                retention_periods=24,
                auto_create_partitions=True,
                auto_drop_old_partitions=True,
                partition_prefix="sales_hybrid_",
                compression=True,
                indexes=[
                    "created_at, customer_id",
                    "customer_id, created_at", 
                    "amount, created_at"
                ]
            )
        ]

    async def initialize_partitioning(self, register_defaults: bool = True):
        """Initialize partitioning system with default configurations."""
        try:
            if register_defaults:
                default_configs = self.get_default_partition_configs()
                for config in default_configs:
                    self.register_partition_config(config)

            # Create partition management schema
            await self._create_partition_management_schema()
            
            # Load existing partition information
            await self._load_partition_registry()
            
            logger.info(f"Initialized partitioning manager with {len(self.partition_configs)} table configs")
            
        except Exception as e:
            logger.error(f"Failed to initialize partitioning manager: {e}")
            raise

    async def _create_partition_management_schema(self):
        """Create schema for partition management metadata."""
        schema_sql = """
        -- Partition registry table
        CREATE TABLE IF NOT EXISTS partition_registry (
            partition_name VARCHAR(255) PRIMARY KEY,
            parent_table VARCHAR(255) NOT NULL,
            strategy VARCHAR(50) NOT NULL,
            partition_key VARCHAR(255) NOT NULL,
            bounds JSONB,
            state VARCHAR(50) DEFAULT 'active',
            size_bytes BIGINT DEFAULT 0,
            row_count BIGINT DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            last_analyzed TIMESTAMP,
            data_range_start VARCHAR(255),
            data_range_end VARCHAR(255),
            metadata JSONB DEFAULT '{}'
        );

        -- Partition maintenance log
        CREATE TABLE IF NOT EXISTS partition_maintenance_log (
            id SERIAL PRIMARY KEY,
            operation VARCHAR(100) NOT NULL,
            table_name VARCHAR(255) NOT NULL,
            partition_name VARCHAR(255),
            operation_details JSONB,
            duration_seconds DECIMAL(10,3),
            status VARCHAR(50) NOT NULL,
            error_message TEXT,
            executed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Partition performance metrics
        CREATE TABLE IF NOT EXISTS partition_performance_metrics (
            metric_id SERIAL PRIMARY KEY,
            table_name VARCHAR(255) NOT NULL,
            partition_name VARCHAR(255),
            query_type VARCHAR(100),
            execution_time_ms DECIMAL(10,3),
            rows_scanned BIGINT,
            partitions_pruned INTEGER,
            partition_elimination_pct DECIMAL(5,2),
            recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        -- Create indexes for partition management
        CREATE INDEX IF NOT EXISTS idx_partition_registry_parent_table 
            ON partition_registry(parent_table, state);
        CREATE INDEX IF NOT EXISTS idx_partition_maintenance_log_table 
            ON partition_maintenance_log(table_name, executed_at);
        CREATE INDEX IF NOT EXISTS idx_partition_performance_metrics_table 
            ON partition_performance_metrics(table_name, recorded_at);
        """
        
        async with self.engine.begin() as conn:
            await conn.execute(text(schema_sql))

    async def _load_partition_registry(self):
        """Load existing partition information from registry."""
        try:
            async with self.engine.begin() as conn:
                result = await conn.execute(text("""
                    SELECT 
                        partition_name, parent_table, strategy, bounds,
                        state, size_bytes, row_count, created_at, last_analyzed,
                        data_range_start, data_range_end
                    FROM partition_registry
                    ORDER BY parent_table, partition_name
                """))
                
                for row in result:
                    parent_table = row[1]
                    if parent_table not in self.partition_registry:
                        self.partition_registry[parent_table] = []
                    
                    partition_info = PartitionInfo(
                        name=row[0],
                        parent_table=parent_table,
                        strategy=PartitionStrategy(row[2]),
                        bounds=row[3] or {},
                        state=PartitionState(row[4]),
                        size_bytes=row[5] or 0,
                        row_count=row[6] or 0,
                        created_at=row[7],
                        last_analyzed=row[8],
                        data_range=(row[9], row[10]) if row[9] and row[10] else None
                    )
                    
                    self.partition_registry[parent_table].append(partition_info)
                    
        except Exception as e:
            logger.warning(f"Could not load partition registry: {e}")

    async def create_partitions_for_table(self, table_name: str) -> Dict[str, Any]:
        """Create partitions for a specific table based on its configuration."""
        if table_name not in self.partition_configs:
            raise ValueError(f"No partition configuration found for table: {table_name}")
        
        config = self.partition_configs[table_name]
        results = {"created": [], "failed": [], "skipped": []}
        
        try:
            if config.strategy == PartitionStrategy.TIME_SERIES:
                results = await self._create_time_series_partitions(config)
            elif config.strategy == PartitionStrategy.HASH:
                results = await self._create_hash_partitions(config)
            elif config.strategy == PartitionStrategy.HYBRID:
                results = await self._create_hybrid_partitions(config)
            else:
                results["failed"].append(f"Strategy {config.strategy} not implemented")

            # Update metrics
            self.metrics["partitions_created"] += len(results["created"])
            
            # Log maintenance operation
            await self._log_maintenance_operation(
                operation="create_partitions",
                table_name=table_name,
                details=results
            )
            
            logger.info(f"Created {len(results['created'])} partitions for {table_name}")
            
        except Exception as e:
            logger.error(f"Failed to create partitions for {table_name}: {e}")
            results["failed"].append(str(e))
            
        return results

    async def _create_time_series_partitions(self, config: PartitionConfig) -> Dict[str, Any]:
        """Create time-series partitions."""
        results = {"created": [], "failed": [], "skipped": []}
        
        try:
            # Determine date range for partition creation
            start_date = datetime.now() - timedelta(days=30)  # Start from 30 days ago
            end_date = datetime.now() + timedelta(days=90)    # Create 90 days ahead
            
            current_date = start_date
            
            while current_date <= end_date:
                try:
                    partition_name, bounds = self._get_time_partition_info(
                        config, current_date
                    )
                    
                    # Check if partition already exists
                    if await self._partition_exists(partition_name):
                        results["skipped"].append(partition_name)
                        current_date = self._get_next_period_date(current_date, config.interval)
                        continue
                    
                    # Create the partition
                    await self._create_time_partition(config, partition_name, bounds)
                    results["created"].append(partition_name)
                    
                    # Register the partition
                    await self._register_partition(
                        partition_name, config.table_name, config.strategy, bounds
                    )
                    
                except Exception as e:
                    results["failed"].append(f"{partition_name}: {str(e)}")
                
                current_date = self._get_next_period_date(current_date, config.interval)
                
        except Exception as e:
            results["failed"].append(str(e))
            
        return results

    async def _create_hash_partitions(self, config: PartitionConfig) -> Dict[str, Any]:
        """Create hash partitions."""
        results = {"created": [], "failed": [], "skipped": []}
        
        try:
            # Create parent partitioned table
            parent_table_sql = f"""
            CREATE TABLE IF NOT EXISTS {config.table_name} (
                LIKE {config.table_name.replace('_partitioned', '').replace('_hash', '')} 
                INCLUDING DEFAULTS INCLUDING CONSTRAINTS INCLUDING INDEXES
            ) PARTITION BY HASH ({config.partition_key})
            """
            
            async with self.engine.begin() as conn:
                await conn.execute(text(parent_table_sql))
            
            # Create hash partitions
            for i in range(config.hash_partitions):
                partition_name = f"{config.partition_prefix}{i:03d}"
                
                try:
                    if await self._partition_exists(partition_name):
                        results["skipped"].append(partition_name)
                        continue
                    
                    partition_sql = f"""
                    CREATE TABLE {partition_name} 
                    PARTITION OF {config.table_name}
                    FOR VALUES WITH (modulus {config.hash_partitions}, remainder {i})
                    """
                    
                    async with self.engine.begin() as conn:
                        await conn.execute(text(partition_sql))
                        
                        # Create indexes on partition
                        for index_cols in config.indexes:
                            index_name = f"idx_{partition_name}_{hashlib.md5(index_cols.encode()).hexdigest()[:8]}"
                            index_sql = f"CREATE INDEX {index_name} ON {partition_name} ({index_cols})"
                            await conn.execute(text(index_sql))
                    
                    bounds = {"modulus": config.hash_partitions, "remainder": i}
                    await self._register_partition(
                        partition_name, config.table_name, config.strategy, bounds
                    )
                    
                    results["created"].append(partition_name)
                    
                except Exception as e:
                    results["failed"].append(f"{partition_name}: {str(e)}")
                    
        except Exception as e:
            results["failed"].append(str(e))
            
        return results

    async def _create_hybrid_partitions(self, config: PartitionConfig) -> Dict[str, Any]:
        """Create hybrid partitions (time + hash)."""
        results = {"created": [], "failed": [], "skipped": []}
        
        # First create time-based parent partitions, then hash sub-partitions
        try:
            start_date = datetime.now() - timedelta(days=30)
            end_date = datetime.now() + timedelta(days=90)
            
            current_date = start_date
            
            while current_date <= end_date:
                try:
                    time_partition_name, time_bounds = self._get_time_partition_info(
                        config, current_date
                    )
                    
                    # Create time-based parent partition
                    if not await self._partition_exists(time_partition_name):
                        partition_keys = config.partition_key.split(',')
                        time_key = partition_keys[0].strip()
                        hash_key = partition_keys[1].strip() if len(partition_keys) > 1 else "id"
                        
                        parent_sql = f"""
                        CREATE TABLE {time_partition_name} 
                        PARTITION OF {config.table_name}
                        FOR VALUES FROM ('{time_bounds["start"]}') TO ('{time_bounds["end"]}')
                        PARTITION BY HASH ({hash_key})
                        """
                        
                        async with self.engine.begin() as conn:
                            await conn.execute(text(parent_sql))
                        
                        # Create hash sub-partitions
                        for i in range(config.hash_partitions):
                            sub_partition_name = f"{time_partition_name}_h{i:02d}"
                            
                            sub_partition_sql = f"""
                            CREATE TABLE {sub_partition_name}
                            PARTITION OF {time_partition_name}
                            FOR VALUES WITH (modulus {config.hash_partitions}, remainder {i})
                            """
                            
                            await conn.execute(text(sub_partition_sql))
                            
                            # Create indexes on sub-partition
                            for index_cols in config.indexes:
                                index_name = f"idx_{sub_partition_name}_{hashlib.md5(index_cols.encode()).hexdigest()[:8]}"
                                index_sql = f"CREATE INDEX {index_name} ON {sub_partition_name} ({index_cols})"
                                await conn.execute(text(index_sql))
                            
                            sub_bounds = {**time_bounds, "hash_modulus": config.hash_partitions, "hash_remainder": i}
                            await self._register_partition(
                                sub_partition_name, config.table_name, config.strategy, sub_bounds
                            )
                            
                            results["created"].append(sub_partition_name)
                    
                except Exception as e:
                    results["failed"].append(f"{time_partition_name}: {str(e)}")
                
                current_date = self._get_next_period_date(current_date, config.interval)
                
        except Exception as e:
            results["failed"].append(str(e))
            
        return results

    def _get_time_partition_info(self, config: PartitionConfig, date: datetime) -> Tuple[str, Dict[str, Any]]:
        """Get partition name and bounds for a given date."""
        if config.interval == PartitionInterval.DAILY:
            start = date.replace(hour=0, minute=0, second=0, microsecond=0)
            end = start + timedelta(days=1)
            suffix = start.strftime("%Y%m%d")
        elif config.interval == PartitionInterval.WEEKLY:
            # Start of week (Monday)
            start = date - timedelta(days=date.weekday())
            start = start.replace(hour=0, minute=0, second=0, microsecond=0)
            end = start + timedelta(weeks=1)
            suffix = start.strftime("%Y_w%U")
        elif config.interval == PartitionInterval.MONTHLY:
            start = date.replace(day=1, hour=0, minute=0, second=0, microsecond=0)
            if start.month == 12:
                end = start.replace(year=start.year + 1, month=1)
            else:
                end = start.replace(month=start.month + 1)
            suffix = start.strftime("%Y%m")
        elif config.interval == PartitionInterval.QUARTERLY:
            quarter = ((date.month - 1) // 3) + 1
            start = date.replace(month=((quarter - 1) * 3) + 1, day=1, hour=0, minute=0, second=0, microsecond=0)
            if quarter == 4:
                end = start.replace(year=start.year + 1, month=1)
            else:
                end = start.replace(month=((quarter) * 3) + 1)
            suffix = start.strftime(f"%Y_q{quarter}")
        elif config.interval == PartitionInterval.YEARLY:
            start = date.replace(month=1, day=1, hour=0, minute=0, second=0, microsecond=0)
            end = start.replace(year=start.year + 1)
            suffix = start.strftime("%Y")
        else:
            raise ValueError(f"Unsupported interval: {config.interval}")
        
        partition_name = f"{config.partition_prefix}{suffix}"
        
        # Convert to date_key format if needed
        if config.partition_key == "date_key":
            start_key = int(start.strftime("%Y%m%d"))
            end_key = int(end.strftime("%Y%m%d"))
            bounds = {"start": start_key, "end": end_key}
        else:
            bounds = {"start": start.isoformat(), "end": end.isoformat()}
            
        return partition_name, bounds

    def _get_next_period_date(self, current_date: datetime, interval: PartitionInterval) -> datetime:
        """Get the next period date based on interval."""
        if interval == PartitionInterval.DAILY:
            return current_date + timedelta(days=1)
        elif interval == PartitionInterval.WEEKLY:
            return current_date + timedelta(weeks=1)
        elif interval == PartitionInterval.MONTHLY:
            if current_date.month == 12:
                return current_date.replace(year=current_date.year + 1, month=1)
            else:
                return current_date.replace(month=current_date.month + 1)
        elif interval == PartitionInterval.QUARTERLY:
            return current_date + timedelta(days=90)  # Approximate
        elif interval == PartitionInterval.YEARLY:
            return current_date.replace(year=current_date.year + 1)
        else:
            return current_date + timedelta(days=1)

    async def _create_time_partition(self, config: PartitionConfig, partition_name: str, bounds: Dict[str, Any]):
        """Create a time-based partition."""
        partition_sql = f"""
        CREATE TABLE {partition_name} 
        PARTITION OF {config.table_name}
        FOR VALUES FROM ({bounds['start']}) TO ({bounds['end']})
        """
        
        async with self.engine.begin() as conn:
            await conn.execute(text(partition_sql))
            
            # Create indexes
            for index_cols in config.indexes:
                index_name = f"idx_{partition_name}_{hashlib.md5(index_cols.encode()).hexdigest()[:8]}"
                index_sql = f"CREATE INDEX {index_name} ON {partition_name} ({index_cols})"
                await conn.execute(text(index_sql))
            
            # Add constraints
            for constraint in config.constraints:
                await conn.execute(text(f"ALTER TABLE {partition_name} ADD {constraint}"))
            
            # Set compression if enabled
            if config.compression:
                await conn.execute(text(f"ALTER TABLE {partition_name} SET (compression = on)"))

    async def _partition_exists(self, partition_name: str) -> bool:
        """Check if a partition exists."""
        try:
            async with self.engine.begin() as conn:
                result = await conn.execute(text(f"""
                    SELECT 1 FROM information_schema.tables 
                    WHERE table_name = '{partition_name}'
                """))
                return result.first() is not None
        except:
            return False

    async def _register_partition(self, partition_name: str, parent_table: str, 
                                  strategy: PartitionStrategy, bounds: Dict[str, Any]):
        """Register a partition in the registry."""
        try:
            async with self.engine.begin() as conn:
                await conn.execute(text("""
                    INSERT INTO partition_registry 
                    (partition_name, parent_table, strategy, partition_key, bounds, state)
                    VALUES ($1, $2, $3, $4, $5, $6)
                    ON CONFLICT (partition_name) DO UPDATE SET
                        bounds = EXCLUDED.bounds,
                        state = EXCLUDED.state
                """), partition_name, parent_table, strategy.value, 
                self.partition_configs[parent_table].partition_key, bounds, PartitionState.ACTIVE.value)
                
        except Exception as e:
            logger.warning(f"Could not register partition {partition_name}: {e}")

    async def maintain_partitions(self) -> Dict[str, Any]:
        """Perform comprehensive partition maintenance."""
        maintenance_start = time.time()
        results = {
            "tables_processed": 0,
            "partitions_created": 0,
            "partitions_dropped": 0,
            "partitions_analyzed": 0,
            "errors": []
        }
        
        try:
            for table_name, config in self.partition_configs.items():
                try:
                    # Create new partitions if needed
                    if config.auto_create_partitions:
                        create_results = await self.create_partitions_for_table(table_name)
                        results["partitions_created"] += len(create_results["created"])
                        results["errors"].extend(create_results["failed"])
                    
                    # Drop old partitions if configured
                    if config.auto_drop_old_partitions:
                        drop_results = await self._drop_old_partitions(config)
                        results["partitions_dropped"] += len(drop_results["dropped"])
                        results["errors"].extend(drop_results["failed"])
                    
                    # Update partition statistics
                    analyze_results = await self._analyze_partitions(table_name)
                    results["partitions_analyzed"] += analyze_results["analyzed_count"]
                    
                    results["tables_processed"] += 1
                    
                except Exception as e:
                    results["errors"].append(f"{table_name}: {str(e)}")
                    logger.error(f"Maintenance error for {table_name}: {e}")

            # Update metrics
            duration = time.time() - maintenance_start
            self.metrics["maintenance_runs"] += 1
            self.metrics["last_maintenance"] = datetime.now()
            
            # Log maintenance operation
            await self._log_maintenance_operation(
                operation="full_maintenance",
                table_name="ALL",
                details=results,
                duration=duration
            )
            
            logger.info(f"Partition maintenance completed in {duration:.2f}s: "
                       f"{results['partitions_created']} created, "
                       f"{results['partitions_dropped']} dropped")
            
        except Exception as e:
            results["errors"].append(f"Maintenance failed: {str(e)}")
            logger.error(f"Partition maintenance failed: {e}")
        
        return results

    async def _drop_old_partitions(self, config: PartitionConfig) -> Dict[str, Any]:
        """Drop old partitions based on retention policy."""
        results = {"dropped": [], "failed": []}
        
        try:
            if config.table_name not in self.partition_registry:
                return results
                
            partitions = self.partition_registry[config.table_name]
            
            # Sort partitions by creation date
            partitions.sort(key=lambda p: p.created_at or datetime.min)
            
            # Keep the most recent 'retention_periods' partitions
            if len(partitions) > config.retention_periods:
                partitions_to_drop = partitions[:-config.retention_periods]
                
                for partition in partitions_to_drop:
                    try:
                        await self._drop_partition(partition.name)
                        await self._unregister_partition(partition.name)
                        results["dropped"].append(partition.name)
                        
                        self.metrics["partitions_dropped"] += 1
                        self.metrics["storage_saved_bytes"] += partition.size_bytes
                        
                    except Exception as e:
                        results["failed"].append(f"{partition.name}: {str(e)}")
                        
        except Exception as e:
            results["failed"].append(str(e))
            
        return results

    async def _drop_partition(self, partition_name: str):
        """Drop a partition table."""
        async with self.engine.begin() as conn:
            await conn.execute(text(f"DROP TABLE IF EXISTS {partition_name}"))

    async def _unregister_partition(self, partition_name: str):
        """Unregister a partition from the registry."""
        async with self.engine.begin() as conn:
            await conn.execute(text("""
                UPDATE partition_registry 
                SET state = $1 
                WHERE partition_name = $2
            """), PartitionState.DROPPED.value, partition_name)

    async def _analyze_partitions(self, table_name: str) -> Dict[str, Any]:
        """Analyze partitions and update statistics."""
        results = {"analyzed_count": 0, "errors": []}
        
        try:
            if table_name not in self.partition_registry:
                return results
                
            for partition in self.partition_registry[table_name]:
                try:
                    # Run ANALYZE on the partition
                    async with self.engine.begin() as conn:
                        await conn.execute(text(f"ANALYZE {partition.name}"))
                    
                    # Update partition statistics
                    await self._update_partition_stats(partition.name)
                    results["analyzed_count"] += 1
                    
                except Exception as e:
                    results["errors"].append(f"{partition.name}: {str(e)}")
                    
        except Exception as e:
            results["errors"].append(str(e))
            
        return results

    async def _update_partition_stats(self, partition_name: str):
        """Update partition statistics in the registry."""
        try:
            async with self.engine.begin() as conn:
                # Get table size
                size_result = await conn.execute(text(f"""
                    SELECT pg_total_relation_size('{partition_name}') as size_bytes
                """))
                size_bytes = size_result.scalar() or 0
                
                # Get row count
                count_result = await conn.execute(text(f"SELECT COUNT(*) FROM {partition_name}"))
                row_count = count_result.scalar() or 0
                
                # Update registry
                await conn.execute(text("""
                    UPDATE partition_registry 
                    SET size_bytes = $1, row_count = $2, last_analyzed = $3
                    WHERE partition_name = $4
                """), size_bytes, row_count, datetime.now(), partition_name)
                
        except Exception as e:
            logger.warning(f"Could not update stats for partition {partition_name}: {e}")

    async def _log_maintenance_operation(self, operation: str, table_name: str, 
                                       details: Dict[str, Any], partition_name: str = None,
                                       duration: float = None):
        """Log maintenance operation."""
        try:
            async with self.engine.begin() as conn:
                await conn.execute(text("""
                    INSERT INTO partition_maintenance_log 
                    (operation, table_name, partition_name, operation_details, duration_seconds, status)
                    VALUES ($1, $2, $3, $4, $5, $6)
                """), operation, table_name, partition_name, details, duration, "success")
                
        except Exception as e:
            logger.warning(f"Could not log maintenance operation: {e}")

    async def get_partition_performance_metrics(self, table_name: str = None) -> Dict[str, Any]:
        """Get partition performance metrics."""
        try:
            where_clause = f"WHERE table_name = '{table_name}'" if table_name else ""
            
            async with self.engine.begin() as conn:
                result = await conn.execute(text(f"""
                    SELECT 
                        table_name,
                        COUNT(*) as total_queries,
                        AVG(execution_time_ms) as avg_execution_time,
                        AVG(partition_elimination_pct) as avg_partition_elimination,
                        MAX(partition_elimination_pct) as max_partition_elimination,
                        SUM(CASE WHEN partition_elimination_pct > 50 THEN 1 ELSE 0 END) as efficient_queries
                    FROM partition_performance_metrics 
                    {where_clause}
                    GROUP BY table_name
                    ORDER BY table_name
                """))
                
                metrics = {}
                for row in result:
                    metrics[row[0]] = {
                        "total_queries": row[1],
                        "avg_execution_time_ms": float(row[2]) if row[2] else 0,
                        "avg_partition_elimination_pct": float(row[3]) if row[3] else 0,
                        "max_partition_elimination_pct": float(row[4]) if row[4] else 0,
                        "efficient_queries": row[5],
                        "efficiency_rate": (row[5] / row[1] * 100) if row[1] > 0 else 0
                    }
                
                return {
                    "partition_metrics": metrics,
                    "overall_metrics": self.metrics,
                    "generated_at": datetime.now().isoformat()
                }
                
        except Exception as e:
            logger.error(f"Could not get partition performance metrics: {e}")
            return {"error": str(e)}

    async def optimize_partition_performance(self, table_name: str) -> Dict[str, Any]:
        """Analyze and provide partition performance optimization recommendations."""
        try:
            recommendations = []
            analysis = {}
            
            if table_name not in self.partition_configs:
                return {"error": f"No partition configuration found for {table_name}"}
            
            config = self.partition_configs[table_name]
            partitions = self.partition_registry.get(table_name, [])
            
            # Analyze partition count
            active_partitions = [p for p in partitions if p.state == PartitionState.ACTIVE]
            analysis["partition_count"] = len(active_partitions)
            
            if len(active_partitions) > 50:
                recommendations.append("Consider increasing retention period or partition interval - too many partitions can hurt performance")
            elif len(active_partitions) < 5 and config.strategy == PartitionStrategy.TIME_SERIES:
                recommendations.append("Consider decreasing partition interval for better parallel processing")
            
            # Analyze partition sizes
            if active_partitions:
                sizes = [p.size_bytes for p in active_partitions if p.size_bytes > 0]
                if sizes:
                    avg_size = sum(sizes) / len(sizes)
                    max_size = max(sizes)
                    min_size = min(sizes)
                    
                    analysis["avg_partition_size_mb"] = avg_size / (1024 * 1024)
                    analysis["size_variance"] = (max_size - min_size) / avg_size if avg_size > 0 else 0
                    
                    if analysis["size_variance"] > 2.0:
                        recommendations.append("High size variance between partitions - consider adjusting partition strategy")
                    
                    if max_size > 10 * 1024 * 1024 * 1024:  # 10GB
                        recommendations.append("Some partitions are very large - consider sub-partitioning or shorter intervals")
            
            # Check for unused partitions
            empty_partitions = [p for p in active_partitions if p.row_count == 0]
            if empty_partitions:
                analysis["empty_partitions"] = len(empty_partitions)
                recommendations.append(f"Found {len(empty_partitions)} empty partitions - consider cleanup")
            
            # Get query performance data
            perf_metrics = await self.get_partition_performance_metrics(table_name)
            if table_name in perf_metrics.get("partition_metrics", {}):
                table_metrics = perf_metrics["partition_metrics"][table_name]
                analysis.update(table_metrics)
                
                if table_metrics.get("avg_partition_elimination_pct", 0) < 70:
                    recommendations.append("Low partition elimination rate - review query patterns and indexing strategy")
                
                if table_metrics.get("efficiency_rate", 0) < 80:
                    recommendations.append("Many queries are not efficiently using partitions - consider query optimization")
            
            return {
                "table_name": table_name,
                "analysis": analysis,
                "recommendations": recommendations,
                "partition_config": {
                    "strategy": config.strategy.value,
                    "partition_key": config.partition_key,
                    "interval": config.interval.value if config.interval else None,
                    "retention_periods": config.retention_periods
                }
            }
            
        except Exception as e:
            logger.error(f"Could not optimize partition performance for {table_name}: {e}")
            return {"error": str(e)}

    def get_partition_summary(self) -> Dict[str, Any]:
        """Get comprehensive partition system summary."""
        summary = {
            "configured_tables": len(self.partition_configs),
            "total_partitions": sum(len(partitions) for partitions in self.partition_registry.values()),
            "active_partitions": 0,
            "archived_partitions": 0,
            "total_size_gb": 0,
            "metrics": self.metrics,
            "table_summary": {}
        }
        
        for table_name, partitions in self.partition_registry.items():
            active_partitions = [p for p in partitions if p.state == PartitionState.ACTIVE]
            archived_partitions = [p for p in partitions if p.state == PartitionState.ARCHIVED]
            
            summary["active_partitions"] += len(active_partitions)
            summary["archived_partitions"] += len(archived_partitions)
            
            table_size_bytes = sum(p.size_bytes for p in partitions if p.size_bytes > 0)
            summary["total_size_gb"] += table_size_bytes / (1024**3)
            
            summary["table_summary"][table_name] = {
                "total_partitions": len(partitions),
                "active_partitions": len(active_partitions),
                "archived_partitions": len(archived_partitions),
                "size_gb": table_size_bytes / (1024**3),
                "strategy": self.partition_configs[table_name].strategy.value if table_name in self.partition_configs else "unknown"
            }
        
        return summary

    async def close(self):
        """Close the partitioning manager."""
        try:
            logger.info("Partitioning manager closed successfully")
        except Exception as e:
            logger.warning(f"Error closing partitioning manager: {e}")


# Factory function
def create_partitioning_manager(engine: AsyncEngine) -> DatabasePartitioningManager:
    """Create DatabasePartitioningManager instance."""
    return DatabasePartitioningManager(engine)


# Example usage
async def main():
    """Example usage of database partitioning manager."""
    from sqlalchemy.ext.asyncio import create_async_engine
    
    # Create async engine (example)
    engine = create_async_engine(
        "postgresql+asyncpg://user:password@localhost/database",
        echo=False
    )
    
    manager = create_partitioning_manager(engine)
    
    try:
        # Initialize with default configurations
        await manager.initialize_partitioning(register_defaults=True)
        
        # Perform maintenance
        maintenance_results = await manager.maintain_partitions()
        print("Maintenance Results:")
        print(f"  Tables processed: {maintenance_results['tables_processed']}")
        print(f"  Partitions created: {maintenance_results['partitions_created']}")
        print(f"  Partitions dropped: {maintenance_results['partitions_dropped']}")
        
        # Get performance metrics
        metrics = await manager.get_partition_performance_metrics()
        print(f"\nPerformance Metrics: {metrics}")
        
        # Get system summary
        summary = manager.get_partition_summary()
        print(f"\nPartition System Summary:")
        print(f"  Configured tables: {summary['configured_tables']}")
        print(f"  Total partitions: {summary['total_partitions']}")
        print(f"  Total size: {summary['total_size_gb']:.2f} GB")
        
    finally:
        await manager.close()
        await engine.dispose()


if __name__ == "__main__":
    asyncio.run(main())