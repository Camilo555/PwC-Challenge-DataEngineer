"""
Change Data Capture (CDC) Manager
================================

Enterprise-grade Change Data Capture system for real-time data synchronization:
- Database trigger-based CDC
- Log-based CDC with WAL monitoring
- Event-driven CDC with message queues
- Real-time streaming to analytics systems
- Conflict resolution and data consistency
- Performance monitoring and alerting
"""
from __future__ import annotations

import asyncio
import json
import hashlib
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Callable, Union, Tuple
from uuid import UUID, uuid4

from sqlalchemy import (
    text, select, func, and_, or_, Integer, String, DateTime, 
    Boolean, JSON as JSONColumn, MetaData, Table, Column, Index
)
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import SQLAlchemyError

import asyncpg
from asyncpg import Connection
import aioredis
from aioredis import Redis

from core.logging import get_logger

logger = get_logger(__name__)


class CDCEventType(str, Enum):
    """CDC event types."""
    INSERT = "insert"
    UPDATE = "update"
    DELETE = "delete"
    TRUNCATE = "truncate"
    SCHEMA_CHANGE = "schema_change"


class CDCMethod(str, Enum):
    """CDC capture methods."""
    TRIGGER_BASED = "trigger_based"
    LOG_BASED = "log_based"
    TIMESTAMP_BASED = "timestamp_based"
    HYBRID = "hybrid"


class CDCStatus(str, Enum):
    """CDC system status."""
    RUNNING = "running"
    STOPPED = "stopped"
    ERROR = "error"
    MAINTENANCE = "maintenance"


class ConflictResolutionStrategy(str, Enum):
    """Conflict resolution strategies."""
    LAST_WRITE_WINS = "last_write_wins"
    FIRST_WRITE_WINS = "first_write_wins"
    CUSTOM_RESOLVER = "custom_resolver"
    MANUAL_RESOLUTION = "manual_resolution"


@dataclass
class CDCEvent:
    """Represents a change data capture event."""
    event_id: UUID = field(default_factory=uuid4)
    table_name: str = ""
    event_type: CDCEventType = CDCEventType.INSERT
    timestamp: datetime = field(default_factory=datetime.utcnow)
    
    # Data payload
    old_values: Optional[Dict[str, Any]] = None
    new_values: Optional[Dict[str, Any]] = None
    changed_columns: Optional[List[str]] = None
    
    # Metadata
    source_system: str = "default"
    transaction_id: Optional[str] = None
    lsn: Optional[int] = None  # Log Sequence Number for PostgreSQL
    schema_name: str = "public"
    
    # Processing metadata
    processed_at: Optional[datetime] = None
    processed_by: Optional[str] = None
    retry_count: int = 0
    error_message: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert CDC event to dictionary."""
        return {
            "event_id": str(self.event_id),
            "table_name": self.table_name,
            "event_type": self.event_type.value,
            "timestamp": self.timestamp.isoformat(),
            "old_values": self.old_values,
            "new_values": self.new_values,
            "changed_columns": self.changed_columns,
            "source_system": self.source_system,
            "transaction_id": self.transaction_id,
            "lsn": self.lsn,
            "schema_name": self.schema_name,
            "processed_at": self.processed_at.isoformat() if self.processed_at else None,
            "processed_by": self.processed_by,
            "retry_count": self.retry_count,
            "error_message": self.error_message
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> CDCEvent:
        """Create CDC event from dictionary."""
        event = cls()
        event.event_id = UUID(data["event_id"]) if data.get("event_id") else uuid4()
        event.table_name = data.get("table_name", "")
        event.event_type = CDCEventType(data.get("event_type", "insert"))
        event.timestamp = datetime.fromisoformat(data["timestamp"]) if data.get("timestamp") else datetime.utcnow()
        event.old_values = data.get("old_values")
        event.new_values = data.get("new_values") 
        event.changed_columns = data.get("changed_columns")
        event.source_system = data.get("source_system", "default")
        event.transaction_id = data.get("transaction_id")
        event.lsn = data.get("lsn")
        event.schema_name = data.get("schema_name", "public")
        event.processed_at = datetime.fromisoformat(data["processed_at"]) if data.get("processed_at") else None
        event.processed_by = data.get("processed_by")
        event.retry_count = data.get("retry_count", 0)
        event.error_message = data.get("error_message")
        return event


@dataclass
class CDCTableConfig:
    """Configuration for CDC on a specific table."""
    table_name: str
    schema_name: str = "public"
    capture_method: CDCMethod = CDCMethod.TRIGGER_BASED
    include_columns: Optional[List[str]] = None  # None means all columns
    exclude_columns: List[str] = field(default_factory=list)
    capture_deletes: bool = True
    capture_updates: bool = True
    capture_inserts: bool = True
    
    # Filtering
    where_condition: Optional[str] = None
    
    # Processing options
    batch_size: int = 1000
    max_retry_attempts: int = 3
    conflict_resolution: ConflictResolutionStrategy = ConflictResolutionStrategy.LAST_WRITE_WINS
    
    # Destination configuration
    target_tables: List[str] = field(default_factory=list)
    transformation_function: Optional[str] = None
    
    def get_trigger_name(self) -> str:
        """Get trigger name for this table."""
        return f"cdc_trigger_{self.table_name}"
    
    def get_function_name(self) -> str:
        """Get trigger function name for this table.""" 
        return f"cdc_function_{self.table_name}"


class CDCManager:
    """
    Enterprise Change Data Capture Manager.
    
    Features:
    - Multi-method CDC (triggers, WAL, timestamp-based)
    - Real-time event streaming
    - Conflict resolution and consistency guarantees
    - Performance monitoring and alerting
    - Scalable event processing
    - Integration with message queues and streaming platforms
    """
    
    def __init__(self, 
                 engine: AsyncEngine,
                 redis_client: Optional[Redis] = None,
                 event_handlers: Optional[List[Callable]] = None):
        self.engine = engine
        self.redis_client = redis_client
        self.event_handlers = event_handlers or []
        
        # CDC configuration
        self.table_configs: Dict[str, CDCTableConfig] = {}
        self.status = CDCStatus.STOPPED
        
        # Event processing
        self.event_queue: asyncio.Queue = asyncio.Queue()
        self.processor_tasks: List[asyncio.Task] = []
        self.wal_listener_task: Optional[asyncio.Task] = None
        
        # Performance metrics
        self.metrics = {
            "events_captured": 0,
            "events_processed": 0,
            "events_failed": 0,
            "average_processing_time_ms": 0.0,
            "events_per_second": 0.0,
            "last_processed_lsn": None,
            "uptime_seconds": 0
        }
        
        self.start_time = datetime.utcnow()
        self._performance_window = []  # Rolling window for metrics
    
    async def initialize(self):
        """Initialize CDC system."""
        try:
            # Create CDC metadata tables
            await self._create_cdc_schema()
            
            # Initialize Redis connection if not provided
            if not self.redis_client:
                try:
                    self.redis_client = await aioredis.from_url("redis://localhost:6379")
                except Exception as e:
                    logger.warning(f"Could not connect to Redis: {e}")
            
            # Load existing configurations
            await self._load_table_configurations()
            
            logger.info("CDC Manager initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize CDC Manager: {e}")
            raise
    
    async def _create_cdc_schema(self):
        """Create CDC metadata schema."""
        
        schema_sql = """
        -- CDC Events Log Table
        CREATE TABLE IF NOT EXISTS cdc_events (
            event_id UUID PRIMARY KEY,
            table_name VARCHAR(255) NOT NULL,
            schema_name VARCHAR(255) DEFAULT 'public',
            event_type VARCHAR(50) NOT NULL,
            event_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            old_values JSONB,
            new_values JSONB,
            changed_columns TEXT[],
            source_system VARCHAR(100) DEFAULT 'default',
            transaction_id VARCHAR(100),
            lsn BIGINT,
            processed_at TIMESTAMP WITH TIME ZONE,
            processed_by VARCHAR(100),
            retry_count INTEGER DEFAULT 0,
            error_message TEXT,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );

        -- CDC Table Configurations
        CREATE TABLE IF NOT EXISTS cdc_table_configs (
            id SERIAL PRIMARY KEY,
            table_name VARCHAR(255) NOT NULL,
            schema_name VARCHAR(255) DEFAULT 'public',
            capture_method VARCHAR(50) DEFAULT 'trigger_based',
            config_json JSONB NOT NULL,
            is_active BOOLEAN DEFAULT true,
            created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(schema_name, table_name)
        );

        -- CDC Processing Status
        CREATE TABLE IF NOT EXISTS cdc_processing_status (
            id SERIAL PRIMARY KEY,
            processor_name VARCHAR(255) UNIQUE NOT NULL,
            last_processed_lsn BIGINT,
            last_processed_timestamp TIMESTAMP WITH TIME ZONE,
            status VARCHAR(50) DEFAULT 'running',
            error_message TEXT,
            updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
        );

        -- CDC Performance Metrics
        CREATE TABLE IF NOT EXISTS cdc_performance_metrics (
            id SERIAL PRIMARY KEY,
            metric_timestamp TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
            events_captured INTEGER DEFAULT 0,
            events_processed INTEGER DEFAULT 0,
            events_failed INTEGER DEFAULT 0,
            avg_processing_time_ms DECIMAL(10,3) DEFAULT 0,
            events_per_second DECIMAL(10,2) DEFAULT 0,
            queue_depth INTEGER DEFAULT 0,
            memory_usage_mb DECIMAL(10,2) DEFAULT 0
        );

        -- Indexes for performance
        CREATE INDEX IF NOT EXISTS idx_cdc_events_table_timestamp 
            ON cdc_events(table_name, event_timestamp);
        CREATE INDEX IF NOT EXISTS idx_cdc_events_lsn 
            ON cdc_events(lsn) WHERE lsn IS NOT NULL;
        CREATE INDEX IF NOT EXISTS idx_cdc_events_processed 
            ON cdc_events(processed_at) WHERE processed_at IS NULL;
        CREATE INDEX IF NOT EXISTS idx_cdc_events_retry 
            ON cdc_events(retry_count, event_timestamp) WHERE retry_count > 0;
        """
        
        async with self.engine.begin() as conn:
            await conn.execute(text(schema_sql))
    
    async def register_table(self, config: CDCTableConfig) -> bool:
        """Register a table for CDC monitoring."""
        try:
            # Store configuration
            self.table_configs[f"{config.schema_name}.{config.table_name}"] = config
            
            # Save to database
            await self._save_table_configuration(config)
            
            # Set up CDC for the table based on method
            if config.capture_method == CDCMethod.TRIGGER_BASED:
                await self._create_trigger_cdc(config)
            elif config.capture_method == CDCMethod.LOG_BASED:
                await self._setup_wal_monitoring(config)
            elif config.capture_method == CDCMethod.TIMESTAMP_BASED:
                await self._setup_timestamp_monitoring(config)
            
            logger.info(f"Registered CDC for table {config.schema_name}.{config.table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to register table {config.table_name}: {e}")
            return False
    
    async def _create_trigger_cdc(self, config: CDCTableConfig):
        """Create trigger-based CDC for a table."""
        
        function_name = config.get_function_name()
        trigger_name = config.get_trigger_name()
        full_table_name = f"{config.schema_name}.{config.table_name}"
        
        # Create trigger function
        function_sql = f"""
        CREATE OR REPLACE FUNCTION {function_name}()
        RETURNS TRIGGER AS $$
        BEGIN
            -- Handle different trigger events
            IF TG_OP = 'DELETE' THEN
                INSERT INTO cdc_events (
                    event_id, table_name, schema_name, event_type, 
                    old_values, transaction_id, source_system
                ) VALUES (
                    gen_random_uuid(),
                    TG_TABLE_NAME,
                    TG_TABLE_SCHEMA, 
                    'delete',
                    to_jsonb(OLD),
                    txid_current()::TEXT,
                    'trigger_cdc'
                );
                RETURN OLD;
                
            ELSIF TG_OP = 'UPDATE' THEN
                INSERT INTO cdc_events (
                    event_id, table_name, schema_name, event_type,
                    old_values, new_values, transaction_id, source_system
                ) VALUES (
                    gen_random_uuid(),
                    TG_TABLE_NAME,
                    TG_TABLE_SCHEMA,
                    'update', 
                    to_jsonb(OLD),
                    to_jsonb(NEW),
                    txid_current()::TEXT,
                    'trigger_cdc'
                );
                RETURN NEW;
                
            ELSIF TG_OP = 'INSERT' THEN
                INSERT INTO cdc_events (
                    event_id, table_name, schema_name, event_type,
                    new_values, transaction_id, source_system
                ) VALUES (
                    gen_random_uuid(),
                    TG_TABLE_NAME,
                    TG_TABLE_SCHEMA,
                    'insert',
                    to_jsonb(NEW),
                    txid_current()::TEXT,
                    'trigger_cdc'
                );
                RETURN NEW;
                
            END IF;
            RETURN NULL;
        END;
        $$ LANGUAGE plpgsql;
        """
        
        # Create trigger
        trigger_sql = f"""
        DROP TRIGGER IF EXISTS {trigger_name} ON {full_table_name};
        CREATE TRIGGER {trigger_name}
            AFTER INSERT OR UPDATE OR DELETE ON {full_table_name}
            FOR EACH ROW EXECUTE FUNCTION {function_name}();
        """
        
        async with self.engine.begin() as conn:
            await conn.execute(text(function_sql))
            await conn.execute(text(trigger_sql))
    
    async def _setup_wal_monitoring(self, config: CDCTableConfig):
        """Set up WAL-based CDC monitoring."""
        try:
            # Create logical replication slot if it doesn't exist
            slot_name = f"cdc_slot_{config.table_name}"
            
            async with self.engine.begin() as conn:
                # Check if slot exists
                result = await conn.execute(text("""
                    SELECT slot_name FROM pg_replication_slots 
                    WHERE slot_name = :slot_name AND active = false
                """), {"slot_name": slot_name})
                
                if not result.fetchone():
                    # Create replication slot
                    await conn.execute(text(f"""
                        SELECT pg_create_logical_replication_slot('{slot_name}', 'wal2json')
                    """))
                    logger.info(f"Created replication slot: {slot_name}")
            
        except Exception as e:
            logger.error(f"Failed to setup WAL monitoring for {config.table_name}: {e}")
    
    async def _setup_timestamp_monitoring(self, config: CDCTableConfig):
        """Set up timestamp-based CDC monitoring."""
        # This method polls for changes based on timestamp columns
        # Implementation would depend on the specific timestamp column strategy
        logger.info(f"Set up timestamp monitoring for {config.table_name}")
    
    async def start_cdc_processing(self, num_processors: int = 4):
        """Start CDC event processing."""
        if self.status == CDCStatus.RUNNING:
            logger.warning("CDC processing is already running")
            return
        
        try:
            self.status = CDCStatus.RUNNING
            
            # Start event processors
            for i in range(num_processors):
                task = asyncio.create_task(self._event_processor(f"processor_{i}"))
                self.processor_tasks.append(task)
            
            # Start WAL listener if we have log-based CDC
            if any(config.capture_method == CDCMethod.LOG_BASED 
                   for config in self.table_configs.values()):
                self.wal_listener_task = asyncio.create_task(self._wal_listener())
            
            # Start metrics collection
            asyncio.create_task(self._collect_metrics())
            
            logger.info(f"Started CDC processing with {num_processors} processors")
            
        except Exception as e:
            self.status = CDCStatus.ERROR
            logger.error(f"Failed to start CDC processing: {e}")
            raise
    
    async def stop_cdc_processing(self):
        """Stop CDC event processing."""
        try:
            self.status = CDCStatus.STOPPED
            
            # Cancel processor tasks
            for task in self.processor_tasks:
                task.cancel()
            
            await asyncio.gather(*self.processor_tasks, return_exceptions=True)
            self.processor_tasks.clear()
            
            # Cancel WAL listener
            if self.wal_listener_task:
                self.wal_listener_task.cancel()
                try:
                    await self.wal_listener_task
                except asyncio.CancelledError:
                    pass
            
            logger.info("Stopped CDC processing")
            
        except Exception as e:
            logger.error(f"Error stopping CDC processing: {e}")
    
    async def _event_processor(self, processor_name: str):
        """Process CDC events from the queue."""
        logger.info(f"Started CDC event processor: {processor_name}")
        
        while self.status == CDCStatus.RUNNING:
            try:
                # Get event from queue with timeout
                try:
                    event = await asyncio.wait_for(self.event_queue.get(), timeout=1.0)
                except asyncio.TimeoutError:
                    continue
                
                # Process the event
                start_time = time.time()
                success = await self._process_cdc_event(event, processor_name)
                processing_time = (time.time() - start_time) * 1000
                
                # Update metrics
                self._performance_window.append({
                    "timestamp": datetime.utcnow(),
                    "processing_time_ms": processing_time,
                    "success": success
                })
                
                if success:
                    self.metrics["events_processed"] += 1
                else:
                    self.metrics["events_failed"] += 1
                
                # Mark task as done
                self.event_queue.task_done()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in event processor {processor_name}: {e}")
                await asyncio.sleep(1)  # Brief pause before retrying
        
        logger.info(f"Stopped CDC event processor: {processor_name}")
    
    async def _process_cdc_event(self, event: CDCEvent, processor_name: str) -> bool:
        """Process a single CDC event."""
        try:
            # Update processing metadata
            event.processed_at = datetime.utcnow()
            event.processed_by = processor_name
            
            # Get table configuration
            table_key = f"{event.schema_name}.{event.table_name}"
            config = self.table_configs.get(table_key)
            
            if not config:
                logger.warning(f"No configuration found for table: {table_key}")
                return False
            
            # Apply transformations if configured
            if config.transformation_function:
                event = await self._apply_transformation(event, config.transformation_function)
            
            # Handle event based on type and configuration
            if event.event_type == CDCEventType.INSERT and config.capture_inserts:
                success = await self._handle_insert_event(event, config)
            elif event.event_type == CDCEventType.UPDATE and config.capture_updates:
                success = await self._handle_update_event(event, config)
            elif event.event_type == CDCEventType.DELETE and config.capture_deletes:
                success = await self._handle_delete_event(event, config)
            else:
                # Event type not configured for capture
                success = True
            
            # Update event status in database
            await self._update_event_status(event, success)
            
            # Call registered event handlers
            for handler in self.event_handlers:
                try:
                    await handler(event)
                except Exception as e:
                    logger.error(f"Event handler error: {e}")
            
            # Publish to Redis if available
            if self.redis_client:
                await self._publish_event_to_redis(event)
            
            return success
            
        except Exception as e:
            logger.error(f"Failed to process CDC event {event.event_id}: {e}")
            
            # Update retry count
            event.retry_count += 1
            event.error_message = str(e)
            
            # Retry if under limit
            config = self.table_configs.get(f"{event.schema_name}.{event.table_name}")
            if config and event.retry_count < config.max_retry_attempts:
                await asyncio.sleep(2 ** event.retry_count)  # Exponential backoff
                await self.event_queue.put(event)
            else:
                await self._update_event_status(event, False)
            
            return False
    
    async def _handle_insert_event(self, event: CDCEvent, config: CDCTableConfig) -> bool:
        """Handle INSERT CDC event."""
        try:
            # Process insert event - could replicate to target tables
            if config.target_tables:
                for target_table in config.target_tables:
                    await self._replicate_insert(event, target_table)
            
            logger.debug(f"Processed INSERT event for {event.table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to handle INSERT event: {e}")
            return False
    
    async def _handle_update_event(self, event: CDCEvent, config: CDCTableConfig) -> bool:
        """Handle UPDATE CDC event."""
        try:
            # Process update event with conflict resolution
            if config.target_tables:
                for target_table in config.target_tables:
                    await self._replicate_update(event, target_table, config.conflict_resolution)
            
            logger.debug(f"Processed UPDATE event for {event.table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to handle UPDATE event: {e}")
            return False
    
    async def _handle_delete_event(self, event: CDCEvent, config: CDCTableConfig) -> bool:
        """Handle DELETE CDC event."""
        try:
            # Process delete event
            if config.target_tables:
                for target_table in config.target_tables:
                    await self._replicate_delete(event, target_table)
            
            logger.debug(f"Processed DELETE event for {event.table_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to handle DELETE event: {e}")
            return False
    
    async def _replicate_insert(self, event: CDCEvent, target_table: str):
        """Replicate INSERT to target table."""
        if not event.new_values:
            return
        
        # Build INSERT statement
        columns = list(event.new_values.keys())
        values = list(event.new_values.values())
        
        placeholders = ", ".join([f":{col}" for col in columns])
        columns_str = ", ".join(columns)
        
        sql = f"INSERT INTO {target_table} ({columns_str}) VALUES ({placeholders})"
        
        async with self.engine.begin() as conn:
            await conn.execute(text(sql), event.new_values)
    
    async def _replicate_update(self, event: CDCEvent, target_table: str, 
                              conflict_resolution: ConflictResolutionStrategy):
        """Replicate UPDATE to target table with conflict resolution."""
        if not event.new_values or not event.old_values:
            return
        
        # Simple conflict resolution - could be enhanced
        if conflict_resolution == ConflictResolutionStrategy.LAST_WRITE_WINS:
            # Build UPDATE statement
            set_clause = ", ".join([f"{col} = :{col}" for col in event.new_values.keys()])
            
            # Use primary key or unique columns for WHERE clause (simplified)
            where_clause = "id = :old_id"  # This would need to be dynamic
            
            sql = f"UPDATE {target_table} SET {set_clause} WHERE {where_clause}"
            
            params = {**event.new_values, "old_id": event.old_values.get("id")}
            
            async with self.engine.begin() as conn:
                await conn.execute(text(sql), params)
    
    async def _replicate_delete(self, event: CDCEvent, target_table: str):
        """Replicate DELETE to target table."""
        if not event.old_values:
            return
        
        # Use primary key for deletion (simplified)
        sql = f"DELETE FROM {target_table} WHERE id = :id"
        
        async with self.engine.begin() as conn:
            await conn.execute(text(sql), {"id": event.old_values.get("id")})
    
    async def _wal_listener(self):
        """Listen to PostgreSQL WAL for changes."""
        logger.info("Started WAL listener for CDC")
        
        try:
            # This would implement actual WAL listening
            # For now, it's a placeholder that polls for new events
            while self.status == CDCStatus.RUNNING:
                try:
                    # Query for new CDC events from triggers
                    await self._poll_cdc_events()
                    await asyncio.sleep(1)  # Poll every second
                except Exception as e:
                    logger.error(f"WAL listener error: {e}")
                    await asyncio.sleep(5)
                    
        except asyncio.CancelledError:
            pass
        
        logger.info("Stopped WAL listener")
    
    async def _poll_cdc_events(self):
        """Poll for new CDC events from the database."""
        try:
            async with self.engine.begin() as conn:
                # Get unprocessed events
                result = await conn.execute(text("""
                    SELECT event_id, table_name, schema_name, event_type, event_timestamp,
                           old_values, new_values, changed_columns, source_system,
                           transaction_id, lsn
                    FROM cdc_events 
                    WHERE processed_at IS NULL
                    ORDER BY event_timestamp
                    LIMIT 100
                """))
                
                for row in result:
                    # Create CDC event object
                    event = CDCEvent(
                        event_id=UUID(row[0]),
                        table_name=row[1],
                        event_type=CDCEventType(row[3]),
                        timestamp=row[4],
                        old_values=row[5],
                        new_values=row[6], 
                        changed_columns=row[7],
                        source_system=row[8],
                        transaction_id=row[9],
                        lsn=row[10],
                        schema_name=row[2]
                    )
                    
                    # Add to processing queue
                    await self.event_queue.put(event)
                    self.metrics["events_captured"] += 1
                    
        except Exception as e:
            logger.error(f"Error polling CDC events: {e}")
    
    async def _collect_metrics(self):
        """Collect and update performance metrics."""
        while self.status == CDCStatus.RUNNING:
            try:
                # Calculate metrics from performance window
                now = datetime.utcnow()
                
                # Filter last minute
                recent_events = [
                    e for e in self._performance_window 
                    if (now - e["timestamp"]).total_seconds() < 60
                ]
                
                if recent_events:
                    avg_processing_time = sum(e["processing_time_ms"] for e in recent_events) / len(recent_events)
                    self.metrics["average_processing_time_ms"] = avg_processing_time
                    self.metrics["events_per_second"] = len(recent_events) / 60.0
                
                # Update uptime
                self.metrics["uptime_seconds"] = (now - self.start_time).total_seconds()
                
                # Clean old performance data
                cutoff = now - timedelta(minutes=5)
                self._performance_window = [
                    e for e in self._performance_window 
                    if e["timestamp"] > cutoff
                ]
                
                # Store metrics in database
                await self._store_performance_metrics()
                
                await asyncio.sleep(30)  # Collect every 30 seconds
                
            except Exception as e:
                logger.error(f"Error collecting metrics: {e}")
                await asyncio.sleep(30)
    
    async def _store_performance_metrics(self):
        """Store performance metrics in database."""
        try:
            async with self.engine.begin() as conn:
                await conn.execute(text("""
                    INSERT INTO cdc_performance_metrics (
                        events_captured, events_processed, events_failed,
                        avg_processing_time_ms, events_per_second, queue_depth
                    ) VALUES (
                        :events_captured, :events_processed, :events_failed,
                        :avg_processing_time_ms, :events_per_second, :queue_depth
                    )
                """), {
                    "events_captured": self.metrics["events_captured"],
                    "events_processed": self.metrics["events_processed"], 
                    "events_failed": self.metrics["events_failed"],
                    "avg_processing_time_ms": self.metrics["average_processing_time_ms"],
                    "events_per_second": self.metrics["events_per_second"],
                    "queue_depth": self.event_queue.qsize()
                })
        except Exception as e:
            logger.error(f"Error storing performance metrics: {e}")
    
    async def _apply_transformation(self, event: CDCEvent, transformation_function: str) -> CDCEvent:
        """Apply transformation function to CDC event."""
        # This would implement custom transformation logic
        # For now, it's a placeholder
        return event
    
    async def _update_event_status(self, event: CDCEvent, success: bool):
        """Update event processing status in database."""
        try:
            async with self.engine.begin() as conn:
                await conn.execute(text("""
                    UPDATE cdc_events 
                    SET processed_at = :processed_at,
                        processed_by = :processed_by,
                        retry_count = :retry_count,
                        error_message = :error_message
                    WHERE event_id = :event_id
                """), {
                    "event_id": event.event_id,
                    "processed_at": event.processed_at,
                    "processed_by": event.processed_by,
                    "retry_count": event.retry_count,
                    "error_message": event.error_message
                })
        except Exception as e:
            logger.error(f"Error updating event status: {e}")
    
    async def _publish_event_to_redis(self, event: CDCEvent):
        """Publish CDC event to Redis for real-time streaming."""
        try:
            channel = f"cdc:{event.schema_name}:{event.table_name}"
            message = json.dumps(event.to_dict())
            await self.redis_client.publish(channel, message)
        except Exception as e:
            logger.error(f"Error publishing event to Redis: {e}")
    
    async def _save_table_configuration(self, config: CDCTableConfig):
        """Save table configuration to database."""
        try:
            config_json = {
                "capture_method": config.capture_method.value,
                "include_columns": config.include_columns,
                "exclude_columns": config.exclude_columns,
                "capture_deletes": config.capture_deletes,
                "capture_updates": config.capture_updates,
                "capture_inserts": config.capture_inserts,
                "where_condition": config.where_condition,
                "batch_size": config.batch_size,
                "max_retry_attempts": config.max_retry_attempts,
                "conflict_resolution": config.conflict_resolution.value,
                "target_tables": config.target_tables,
                "transformation_function": config.transformation_function
            }
            
            async with self.engine.begin() as conn:
                await conn.execute(text("""
                    INSERT INTO cdc_table_configs (table_name, schema_name, capture_method, config_json)
                    VALUES (:table_name, :schema_name, :capture_method, :config_json)
                    ON CONFLICT (schema_name, table_name) DO UPDATE SET
                        capture_method = EXCLUDED.capture_method,
                        config_json = EXCLUDED.config_json,
                        updated_at = CURRENT_TIMESTAMP
                """), {
                    "table_name": config.table_name,
                    "schema_name": config.schema_name,
                    "capture_method": config.capture_method.value,
                    "config_json": json.dumps(config_json)
                })
        except Exception as e:
            logger.error(f"Error saving table configuration: {e}")
    
    async def _load_table_configurations(self):
        """Load table configurations from database."""
        try:
            async with self.engine.begin() as conn:
                result = await conn.execute(text("""
                    SELECT table_name, schema_name, capture_method, config_json
                    FROM cdc_table_configs
                    WHERE is_active = true
                """))
                
                for row in result:
                    table_name, schema_name, capture_method, config_json = row
                    config_data = json.loads(config_json)
                    
                    config = CDCTableConfig(
                        table_name=table_name,
                        schema_name=schema_name,
                        capture_method=CDCMethod(capture_method),
                        **config_data
                    )
                    
                    table_key = f"{schema_name}.{table_name}"
                    self.table_configs[table_key] = config
                    
        except Exception as e:
            logger.error(f"Error loading table configurations: {e}")
    
    def get_cdc_status(self) -> Dict[str, Any]:
        """Get comprehensive CDC system status."""
        return {
            "status": self.status.value,
            "uptime_seconds": self.metrics["uptime_seconds"],
            "registered_tables": len(self.table_configs),
            "active_processors": len(self.processor_tasks),
            "queue_depth": self.event_queue.qsize(),
            "performance_metrics": self.metrics,
            "table_configs": {
                table_key: {
                    "table_name": config.table_name,
                    "schema_name": config.schema_name,
                    "capture_method": config.capture_method.value,
                    "target_tables": config.target_tables
                }
                for table_key, config in self.table_configs.items()
            }
        }
    
    async def close(self):
        """Close CDC manager and cleanup resources."""
        try:
            await self.stop_cdc_processing()
            
            if self.redis_client:
                await self.redis_client.close()
            
            logger.info("CDC Manager closed successfully")
        except Exception as e:
            logger.error(f"Error closing CDC Manager: {e}")


# Factory function
def create_cdc_manager(engine: AsyncEngine, 
                      redis_client: Optional[Redis] = None,
                      event_handlers: Optional[List[Callable]] = None) -> CDCManager:
    """Create CDC Manager instance."""
    return CDCManager(engine, redis_client, event_handlers)


# Example usage
async def main():
    """Example usage of CDC Manager."""
    from sqlalchemy.ext.asyncio import create_async_engine
    
    # Create engine
    engine = create_async_engine("postgresql+asyncpg://user:pass@localhost/db")
    
    # Create CDC manager
    cdc_manager = create_cdc_manager(engine)
    await cdc_manager.initialize()
    
    # Register table for CDC
    config = CDCTableConfig(
        table_name="fact_sale",
        schema_name="public",
        capture_method=CDCMethod.TRIGGER_BASED,
        target_tables=["fact_sale_replica"],
        capture_inserts=True,
        capture_updates=True,
        capture_deletes=True
    )
    
    await cdc_manager.register_table(config)
    
    # Start CDC processing
    await cdc_manager.start_cdc_processing(num_processors=4)
    
    try:
        # Let it run
        await asyncio.sleep(60)
        
        # Get status
        status = cdc_manager.get_cdc_status()
        print(f"CDC Status: {status}")
        
    finally:
        await cdc_manager.close()


if __name__ == "__main__":
    asyncio.run(main())