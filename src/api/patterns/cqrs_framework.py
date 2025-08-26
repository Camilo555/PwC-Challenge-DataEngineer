"""
CQRS (Command Query Responsibility Segregation) Framework
Separates command (write) and query (read) operations for better scalability
"""
import asyncio
import json
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Type, Union, Generic, TypeVar

from core.logging import get_logger
from messaging.rabbitmq_manager import RabbitMQManager, TaskMessage, MessagePriority
from streaming.kafka_manager import KafkaManager, StreamingTopic
from core.caching.redis_cache import RedisCache

logger = get_logger(__name__)

T = TypeVar('T')
R = TypeVar('R')


class CommandStatus(Enum):
    """Command execution status"""
    PENDING = "pending"
    EXECUTING = "executing"
    COMPLETED = "completed"
    FAILED = "failed"


class QueryType(Enum):
    """Query operation types"""
    READ = "read"
    SEARCH = "search"
    AGGREGATE = "aggregate"
    ANALYTICS = "analytics"


@dataclass
class CommandResult:
    """Result of command execution"""
    command_id: str
    success: bool
    result: Optional[Dict[str, Any]] = None
    error: Optional[str] = None
    execution_time_ms: Optional[float] = None
    events_generated: List[str] = None
    
    def __post_init__(self):
        if self.events_generated is None:
            self.events_generated = []


@dataclass
class QueryResult:
    """Result of query execution"""
    query_id: str
    data: Any
    total_count: Optional[int] = None
    page: Optional[int] = None
    page_size: Optional[int] = None
    execution_time_ms: Optional[float] = None
    cache_hit: bool = False
    source: Optional[str] = None


@dataclass
class DomainEvent:
    """Domain event for event sourcing"""
    event_id: str
    aggregate_id: str
    aggregate_type: str
    event_type: str
    event_data: Dict[str, Any]
    version: int
    timestamp: float
    correlation_id: Optional[str] = None
    causation_id: Optional[str] = None
    metadata: Optional[Dict[str, Any]] = None
    
    def __post_init__(self):
        if not self.event_id:
            self.event_id = str(uuid.uuid4())
        if self.timestamp is None:
            self.timestamp = time.time()
        if self.metadata is None:
            self.metadata = {}


class Command(ABC):
    """Base class for all commands"""
    
    def __init__(self, command_id: Optional[str] = None, correlation_id: Optional[str] = None):
        self.command_id = command_id or str(uuid.uuid4())
        self.correlation_id = correlation_id or str(uuid.uuid4())
        self.timestamp = time.time()
    
    @abstractmethod
    def validate(self) -> bool:
        """Validate command data"""
        pass


class Query(ABC):
    """Base class for all queries"""
    
    def __init__(self, query_id: Optional[str] = None):
        self.query_id = query_id or str(uuid.uuid4())
        self.timestamp = time.time()
    
    @abstractmethod
    def get_cache_key(self) -> Optional[str]:
        """Get cache key for query results"""
        pass


class CommandHandler(ABC, Generic[T]):
    """Base class for command handlers"""
    
    @abstractmethod
    async def handle(self, command: T) -> CommandResult:
        """Handle the command"""
        pass
    
    @abstractmethod
    def can_handle(self, command: Command) -> bool:
        """Check if this handler can handle the command"""
        pass


class QueryHandler(ABC, Generic[T, R]):
    """Base class for query handlers"""
    
    @abstractmethod
    async def handle(self, query: T) -> QueryResult:
        """Handle the query"""
        pass
    
    @abstractmethod
    def can_handle(self, query: Query) -> bool:
        """Check if this handler can handle the query"""
        pass


class EventStore:
    """Event store for storing domain events"""
    
    def __init__(self):
        self.kafka_manager = KafkaManager()
        self.events: Dict[str, List[DomainEvent]] = {}  # In-memory for demo
    
    async def save_events(self, aggregate_id: str, events: List[DomainEvent], expected_version: int = -1):
        """Save events to the event store"""
        if aggregate_id not in self.events:
            self.events[aggregate_id] = []
        
        current_version = len(self.events[aggregate_id])
        
        if expected_version != -1 and expected_version != current_version:
            raise Exception(f"Concurrency conflict: expected version {expected_version}, got {current_version}")
        
        # Assign version numbers
        for i, event in enumerate(events):
            event.version = current_version + i + 1
        
        # Save to in-memory store
        self.events[aggregate_id].extend(events)
        
        # Publish to Kafka for event streaming
        for event in events:
            await self._publish_event(event)
    
    async def get_events(self, aggregate_id: str, from_version: int = 0) -> List[DomainEvent]:
        """Get events for an aggregate"""
        events = self.events.get(aggregate_id, [])
        return [event for event in events if event.version > from_version]
    
    async def _publish_event(self, event: DomainEvent):
        """Publish event to Kafka"""
        try:
            self.kafka_manager.produce_domain_event(
                aggregate_id=event.aggregate_id,
                aggregate_type=event.aggregate_type,
                event_type=event.event_type,
                event_data=event.event_data,
                version=event.version,
                correlation_id=event.correlation_id
            )
        except Exception as e:
            logger.error(f"Failed to publish event: {e}")


class ReadModelUpdater:
    """Updates read models based on domain events"""
    
    def __init__(self, event_store: EventStore):
        self.event_store = event_store
        self.projectors: Dict[str, List[Callable]] = {}
        self.redis_cache = RedisCache()
        self.last_processed_positions: Dict[str, int] = {}
    
    def register_projector(self, event_type: str, projector: Callable[[DomainEvent], None]):
        """Register a projector for an event type"""
        if event_type not in self.projectors:
            self.projectors[event_type] = []
        self.projectors[event_type].append(projector)
    
    async def process_events(self, aggregate_id: str):
        """Process events for an aggregate and update read models"""
        last_position = self.last_processed_positions.get(aggregate_id, 0)
        events = await self.event_store.get_events(aggregate_id, last_position)
        
        for event in events:
            await self._process_event(event)
            self.last_processed_positions[aggregate_id] = event.version
    
    async def _process_event(self, event: DomainEvent):
        """Process a single event"""
        projectors = self.projectors.get(event.event_type, [])
        
        for projector in projectors:
            try:
                await projector(event)
            except Exception as e:
                logger.error(f"Projector failed for event {event.event_id}: {e}")


class CommandBus:
    """Command bus for routing commands to handlers"""
    
    def __init__(self, event_store: EventStore):
        self.handlers: List[CommandHandler] = []
        self.event_store = event_store
        self.rabbitmq_manager = RabbitMQManager()
        self.middleware: List[Callable] = []
    
    def register_handler(self, handler: CommandHandler):
        """Register a command handler"""
        self.handlers.append(handler)
    
    def add_middleware(self, middleware: Callable):
        """Add middleware for command processing"""
        self.middleware.append(middleware)
    
    async def send(self, command: Command) -> CommandResult:
        """Send a command for processing"""
        start_time = time.time()
        
        try:
            # Validate command
            if not command.validate():
                return CommandResult(
                    command_id=command.command_id,
                    success=False,
                    error="Command validation failed"
                )
            
            # Find handler
            handler = self._find_handler(command)
            if not handler:
                return CommandResult(
                    command_id=command.command_id,
                    success=False,
                    error=f"No handler found for command: {type(command).__name__}"
                )
            
            # Apply middleware
            for middleware in self.middleware:
                await middleware(command)
            
            # Execute command
            result = await handler.handle(command)
            result.execution_time_ms = (time.time() - start_time) * 1000
            
            # Publish command execution event
            await self._publish_command_event(command, result)
            
            return result
            
        except Exception as e:
            logger.error(f"Command execution failed: {e}")
            return CommandResult(
                command_id=command.command_id,
                success=False,
                error=str(e),
                execution_time_ms=(time.time() - start_time) * 1000
            )
    
    def _find_handler(self, command: Command) -> Optional[CommandHandler]:
        """Find handler for command"""
        for handler in self.handlers:
            if handler.can_handle(command):
                return handler
        return None
    
    async def _publish_command_event(self, command: Command, result: CommandResult):
        """Publish command execution event"""
        try:
            event_data = {
                "command_id": command.command_id,
                "command_type": type(command).__name__,
                "success": result.success,
                "execution_time_ms": result.execution_time_ms,
                "correlation_id": command.correlation_id,
                "timestamp": time.time()
            }
            
            if result.error:
                event_data["error"] = result.error
            
            # Publish to RabbitMQ for monitoring
            message = TaskMessage(
                task_id=f"command_executed_{command.command_id}",
                task_name="command_executed",
                payload=event_data,
                priority=MessagePriority.NORMAL,
                correlation_id=command.correlation_id
            )
            
            await self.rabbitmq_manager.publish_task_async(message, "command_events")
            
        except Exception as e:
            logger.error(f"Failed to publish command event: {e}")


class QueryBus:
    """Query bus for routing queries to handlers"""
    
    def __init__(self):
        self.handlers: List[QueryHandler] = []
        self.redis_cache = RedisCache()
        self.middleware: List[Callable] = []
        self.cache_ttl_seconds = 300  # 5 minutes default
    
    def register_handler(self, handler: QueryHandler):
        """Register a query handler"""
        self.handlers.append(handler)
    
    def add_middleware(self, middleware: Callable):
        """Add middleware for query processing"""
        self.middleware.append(middleware)
    
    async def send(self, query: Query) -> QueryResult:
        """Send a query for processing"""
        start_time = time.time()
        
        try:
            # Check cache first
            cache_key = query.get_cache_key()
            if cache_key:
                cached_result = await self.redis_cache.get(cache_key)
                if cached_result:
                    result_data = json.loads(cached_result)
                    return QueryResult(
                        query_id=query.query_id,
                        data=result_data,
                        cache_hit=True,
                        execution_time_ms=(time.time() - start_time) * 1000,
                        source="cache"
                    )
            
            # Find handler
            handler = self._find_handler(query)
            if not handler:
                raise Exception(f"No handler found for query: {type(query).__name__}")
            
            # Apply middleware
            for middleware in self.middleware:
                await middleware(query)
            
            # Execute query
            result = await handler.handle(query)
            result.execution_time_ms = (time.time() - start_time) * 1000
            result.source = "database"
            
            # Cache result if cache key is available
            if cache_key and result.data:
                await self.redis_cache.set(
                    cache_key, 
                    json.dumps(result.data), 
                    self.cache_ttl_seconds
                )
            
            return result
            
        except Exception as e:
            logger.error(f"Query execution failed: {e}")
            raise e
    
    def _find_handler(self, query: Query) -> Optional[QueryHandler]:
        """Find handler for query"""
        for handler in self.handlers:
            if handler.can_handle(query):
                return handler
        return None


class CQRSFramework:
    """Main CQRS framework orchestrating commands, queries, and events"""
    
    def __init__(self):
        self.event_store = EventStore()
        self.command_bus = CommandBus(self.event_store)
        self.query_bus = QueryBus()
        self.read_model_updater = ReadModelUpdater(self.event_store)
        
        # Start background event processing
        self._event_processing_task = None
        self._start_event_processing()
    
    def _start_event_processing(self):
        """Start background event processing"""
        if self._event_processing_task is None or self._event_processing_task.done():
            self._event_processing_task = asyncio.create_task(self._process_events_loop())
    
    async def _process_events_loop(self):
        """Background loop for processing events and updating read models"""
        while True:
            try:
                # Process events for all aggregates
                for aggregate_id in self.event_store.events.keys():
                    await self.read_model_updater.process_events(aggregate_id)
                
                await asyncio.sleep(1)  # Process every second
                
            except Exception as e:
                logger.error(f"Event processing error: {e}")
                await asyncio.sleep(5)
    
    async def execute_command(self, command: Command) -> CommandResult:
        """Execute a command"""
        return await self.command_bus.send(command)
    
    async def execute_query(self, query: Query) -> QueryResult:
        """Execute a query"""
        return await self.query_bus.send(query)
    
    def register_command_handler(self, handler: CommandHandler):
        """Register a command handler"""
        self.command_bus.register_handler(handler)
    
    def register_query_handler(self, handler: QueryHandler):
        """Register a query handler"""
        self.query_bus.register_handler(handler)
    
    def register_event_projector(self, event_type: str, projector: Callable[[DomainEvent], None]):
        """Register an event projector"""
        self.read_model_updater.register_projector(event_type, projector)
    
    async def get_metrics(self) -> Dict[str, Any]:
        """Get CQRS framework metrics"""
        return {
            "command_handlers": len(self.command_bus.handlers),
            "query_handlers": len(self.query_bus.handlers),
            "event_projectors": sum(len(projectors) for projectors in self.read_model_updater.projectors.values()),
            "total_events": sum(len(events) for events in self.event_store.events.values()),
            "aggregates": len(self.event_store.events),
            "cache_enabled": bool(self.query_bus.redis_cache)
        }
    
    async def close(self):
        """Clean up resources"""
        if self._event_processing_task and not self._event_processing_task.done():
            self._event_processing_task.cancel()


# Example implementations for Sales domain

class CreateSaleCommand(Command):
    """Command to create a new sale"""
    
    def __init__(self, sale_data: Dict[str, Any], **kwargs):
        super().__init__(**kwargs)
        self.sale_data = sale_data
    
    def validate(self) -> bool:
        required_fields = ['customer_id', 'product_id', 'quantity', 'unit_price']
        return all(field in self.sale_data for field in required_fields)


class UpdateSaleCommand(Command):
    """Command to update an existing sale"""
    
    def __init__(self, sale_id: str, updates: Dict[str, Any], **kwargs):
        super().__init__(**kwargs)
        self.sale_id = sale_id
        self.updates = updates
    
    def validate(self) -> bool:
        return bool(self.sale_id and self.updates)


class GetSaleQuery(Query):
    """Query to get a sale by ID"""
    
    def __init__(self, sale_id: str, **kwargs):
        super().__init__(**kwargs)
        self.sale_id = sale_id
    
    def get_cache_key(self) -> str:
        return f"sale:{self.sale_id}"


class SearchSalesQuery(Query):
    """Query to search sales with filters"""
    
    def __init__(self, filters: Dict[str, Any], page: int = 1, page_size: int = 50, **kwargs):
        super().__init__(**kwargs)
        self.filters = filters
        self.page = page
        self.page_size = page_size
    
    def get_cache_key(self) -> str:
        # Create cache key from filters
        filter_str = json.dumps(self.filters, sort_keys=True)
        return f"sales_search:{hash(filter_str)}:{self.page}:{self.page_size}"


class SalesAnalyticsQuery(Query):
    """Query for sales analytics"""
    
    def __init__(self, date_range: Dict[str, str], group_by: str = "day", **kwargs):
        super().__init__(**kwargs)
        self.date_range = date_range
        self.group_by = group_by
    
    def get_cache_key(self) -> str:
        return f"sales_analytics:{self.date_range['start']}:{self.date_range['end']}:{self.group_by}"


# Singleton framework instance
_cqrs_framework = None

def get_cqrs_framework() -> CQRSFramework:
    """Get global CQRS framework instance"""
    global _cqrs_framework
    if _cqrs_framework is None:
        _cqrs_framework = CQRSFramework()
    return _cqrs_framework