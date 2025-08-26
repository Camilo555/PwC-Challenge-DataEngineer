"""
CQRS (Command Query Responsibility Segregation) Pattern Implementation
Enterprise-grade CQRS with event sourcing, command/query separation, and consistency management
"""
from __future__ import annotations

import asyncio
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, List, Optional, Type, TypeVar, Generic, Union

from pydantic import BaseModel, Field

from core.logging import get_logger
from messaging.rabbitmq_manager import RabbitMQManager, QueueType, MessagePriority
from streaming.kafka_manager import KafkaManager, StreamingTopic

logger = get_logger(__name__)

T = TypeVar('T')
R = TypeVar('R')


class CommandStatus(Enum):
    """Status of command execution."""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class QueryType(Enum):
    """Types of queries."""
    SIMPLE = "simple"
    AGGREGATION = "aggregation"
    ANALYTICAL = "analytical"
    REPORTING = "reporting"


@dataclass
class CommandResult:
    """Result of command execution."""
    command_id: str
    status: CommandStatus
    result: Any = None
    error: Optional[str] = None
    timestamp: datetime = field(default_factory=datetime.now)
    execution_time_ms: float = 0.0
    events: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class QueryResult(Generic[T]):
    """Result of query execution."""
    query_id: str
    data: T
    metadata: Dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=datetime.now)
    execution_time_ms: float = 0.0
    cache_hit: bool = False


# Base Classes

class Command(BaseModel, ABC):
    """Base class for all commands."""
    command_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = Field(default_factory=datetime.now)
    user_id: Optional[str] = None
    correlation_id: Optional[str] = None
    
    class Config:
        arbitrary_types_allowed = True
    
    @abstractmethod
    def validate_command(self) -> bool:
        """Validate command before execution."""
        pass


class Query(BaseModel, ABC):
    """Base class for all queries."""
    query_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    timestamp: datetime = Field(default_factory=datetime.now)
    user_id: Optional[str] = None
    query_type: QueryType = QueryType.SIMPLE
    
    class Config:
        arbitrary_types_allowed = True


class Event(BaseModel):
    """Domain event for event sourcing."""
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: str
    aggregate_id: str
    aggregate_type: str
    payload: Dict[str, Any]
    timestamp: datetime = Field(default_factory=datetime.now)
    version: int = 1
    correlation_id: Optional[str] = None
    causation_id: Optional[str] = None


# Handlers

class CommandHandler(ABC, Generic[T, R]):
    """Base class for command handlers."""
    
    @abstractmethod
    async def handle(self, command: T) -> CommandResult:
        """Handle the command and return result."""
        pass
    
    @abstractmethod
    def can_handle(self, command: Command) -> bool:
        """Check if this handler can handle the command."""
        pass


class QueryHandler(ABC, Generic[T, R]):
    """Base class for query handlers."""
    
    @abstractmethod
    async def handle(self, query: T) -> QueryResult[R]:
        """Handle the query and return result."""
        pass
    
    @abstractmethod
    def can_handle(self, query: Query) -> bool:
        """Check if this handler can handle the query."""
        pass


class EventHandler(ABC):
    """Base class for event handlers."""
    
    @abstractmethod
    async def handle(self, event: Event) -> None:
        """Handle the event."""
        pass
    
    @abstractmethod
    def can_handle(self, event: Event) -> bool:
        """Check if this handler can handle the event."""
        pass


# CQRS Bus Implementation

class CommandBus:
    """Command bus for routing commands to handlers."""
    
    def __init__(self):
        self.handlers: Dict[Type[Command], CommandHandler] = {}
        self.middleware: List[Any] = []
        self.rabbitmq_manager = RabbitMQManager()
    
    def register_handler(self, command_type: Type[Command], handler: CommandHandler):
        """Register a command handler."""
        self.handlers[command_type] = handler
        logger.info(f"Registered command handler for {command_type.__name__}")
    
    def add_middleware(self, middleware):
        """Add middleware to the command pipeline."""
        self.middleware.append(middleware)
    
    async def send(self, command: Command) -> CommandResult:
        """Send command for processing."""
        start_time = datetime.now()
        
        try:
            # Validate command
            if not command.validate_command():
                return CommandResult(
                    command_id=command.command_id,
                    status=CommandStatus.FAILED,
                    error="Command validation failed"
                )
            
            # Find appropriate handler
            handler = self._find_handler(command)
            if not handler:
                return CommandResult(
                    command_id=command.command_id,
                    status=CommandStatus.FAILED,
                    error=f"No handler found for command {type(command).__name__}"
                )
            
            # Apply middleware
            for mw in self.middleware:
                command = await mw.process(command)
            
            # Execute command
            logger.info(f"Executing command {command.command_id} of type {type(command).__name__}")
            result = await handler.handle(command)
            
            # Calculate execution time
            execution_time = (datetime.now() - start_time).total_seconds() * 1000
            result.execution_time_ms = execution_time
            
            # Publish command completion event
            await self._publish_command_event(command, result)
            
            return result
            
        except Exception as e:
            logger.error(f"Error executing command {command.command_id}: {e}")
            execution_time = (datetime.now() - start_time).total_seconds() * 1000
            
            return CommandResult(
                command_id=command.command_id,
                status=CommandStatus.FAILED,
                error=str(e),
                execution_time_ms=execution_time
            )
    
    def _find_handler(self, command: Command) -> Optional[CommandHandler]:
        """Find handler for command."""
        command_type = type(command)
        if command_type in self.handlers:
            return self.handlers[command_type]
        
        # Check if any handler can handle this command
        for handler in self.handlers.values():
            if handler.can_handle(command):
                return handler
        
        return None
    
    async def _publish_command_event(self, command: Command, result: CommandResult):
        """Publish command execution event."""
        try:
            event_data = {
                "command_id": command.command_id,
                "command_type": type(command).__name__,
                "status": result.status.value,
                "execution_time_ms": result.execution_time_ms,
                "timestamp": datetime.now().isoformat(),
                "user_id": command.user_id,
                "correlation_id": command.correlation_id
            }
            
            # Publish to message queue for processing
            self.rabbitmq_manager.publish_task(
                task_name="command_executed",
                payload=event_data,
                queue=QueueType.EVENT_QUEUE,
                priority=MessagePriority.MEDIUM
            )
            
        except Exception as e:
            logger.error(f"Error publishing command event: {e}")


class QueryBus:
    """Query bus for routing queries to handlers."""
    
    def __init__(self):
        self.handlers: Dict[Type[Query], QueryHandler] = {}
        self.middleware: List[Any] = []
        self.cache_manager = None  # Will be injected
    
    def register_handler(self, query_type: Type[Query], handler: QueryHandler):
        """Register a query handler."""
        self.handlers[query_type] = handler
        logger.info(f"Registered query handler for {query_type.__name__}")
    
    def add_middleware(self, middleware):
        """Add middleware to the query pipeline."""
        self.middleware.append(middleware)
    
    async def send(self, query: Query) -> QueryResult:
        """Send query for processing."""
        start_time = datetime.now()
        
        try:
            # Find appropriate handler
            handler = self._find_handler(query)
            if not handler:
                raise ValueError(f"No handler found for query {type(query).__name__}")
            
            # Check cache first for cacheable queries
            cached_result = await self._get_cached_result(query)
            if cached_result:
                logger.info(f"Cache hit for query {query.query_id}")
                return cached_result
            
            # Apply middleware
            for mw in self.middleware:
                query = await mw.process(query)
            
            # Execute query
            logger.info(f"Executing query {query.query_id} of type {type(query).__name__}")
            result = await handler.handle(query)
            
            # Calculate execution time
            execution_time = (datetime.now() - start_time).total_seconds() * 1000
            result.execution_time_ms = execution_time
            
            # Cache result if applicable
            await self._cache_result(query, result)
            
            return result
            
        except Exception as e:
            logger.error(f"Error executing query {query.query_id}: {e}")
            execution_time = (datetime.now() - start_time).total_seconds() * 1000
            
            # Return error result
            return QueryResult(
                query_id=query.query_id,
                data=None,
                execution_time_ms=execution_time,
                metadata={"error": str(e)}
            )
    
    def _find_handler(self, query: Query) -> Optional[QueryHandler]:
        """Find handler for query."""
        query_type = type(query)
        if query_type in self.handlers:
            return self.handlers[query_type]
        
        # Check if any handler can handle this query
        for handler in self.handlers.values():
            if handler.can_handle(query):
                return handler
        
        return None
    
    async def _get_cached_result(self, query: Query) -> Optional[QueryResult]:
        """Get cached query result if available."""
        if not self.cache_manager:
            return None
        
        try:
            cache_key = f"query:{type(query).__name__}:{hash(str(query.dict()))}"
            cached_data = await self.cache_manager.get(cache_key)
            
            if cached_data:
                result = QueryResult(
                    query_id=query.query_id,
                    data=cached_data["data"],
                    metadata=cached_data.get("metadata", {}),
                    cache_hit=True
                )
                return result
        except Exception as e:
            logger.warning(f"Error getting cached result: {e}")
        
        return None
    
    async def _cache_result(self, query: Query, result: QueryResult):
        """Cache query result."""
        if not self.cache_manager or result.metadata.get("error"):
            return
        
        try:
            cache_key = f"query:{type(query).__name__}:{hash(str(query.dict()))}"
            cache_data = {
                "data": result.data,
                "metadata": result.metadata,
                "timestamp": result.timestamp.isoformat()
            }
            
            # Cache for different durations based on query type
            ttl_mapping = {
                QueryType.SIMPLE: 300,      # 5 minutes
                QueryType.AGGREGATION: 900, # 15 minutes
                QueryType.ANALYTICAL: 1800, # 30 minutes
                QueryType.REPORTING: 3600   # 1 hour
            }
            
            ttl = ttl_mapping.get(query.query_type, 300)
            await self.cache_manager.set(cache_key, cache_data, ttl)
            
        except Exception as e:
            logger.warning(f"Error caching query result: {e}")


class EventBus:
    """Event bus for routing events to handlers."""
    
    def __init__(self):
        self.handlers: List[EventHandler] = []
        self.kafka_manager = KafkaManager()
    
    def register_handler(self, handler: EventHandler):
        """Register an event handler."""
        self.handlers.append(handler)
        logger.info(f"Registered event handler {handler.__class__.__name__}")
    
    async def publish(self, event: Event):
        """Publish event to handlers and event store."""
        try:
            # Store event in event store (Kafka)
            await self._store_event(event)
            
            # Route to all applicable handlers
            handlers_executed = 0
            for handler in self.handlers:
                if handler.can_handle(event):
                    try:
                        await handler.handle(event)
                        handlers_executed += 1
                    except Exception as e:
                        logger.error(f"Error in event handler {handler.__class__.__name__}: {e}")
            
            logger.info(f"Published event {event.event_id} to {handlers_executed} handlers")
            
        except Exception as e:
            logger.error(f"Error publishing event {event.event_id}: {e}")
    
    async def _store_event(self, event: Event):
        """Store event in event store."""
        try:
            event_data = {
                "event_id": event.event_id,
                "event_type": event.event_type,
                "aggregate_id": event.aggregate_id,
                "aggregate_type": event.aggregate_type,
                "payload": event.payload,
                "timestamp": event.timestamp.isoformat(),
                "version": event.version,
                "correlation_id": event.correlation_id,
                "causation_id": event.causation_id
            }
            
            # Store in Kafka for event sourcing
            self.kafka_manager.produce_message(
                topic=StreamingTopic.DOMAIN_EVENTS,
                message=event_data,
                key=event.aggregate_id
            )
            
        except Exception as e:
            logger.error(f"Error storing event in event store: {e}")


# CQRS Mediator

class CQRSMediator:
    """Central mediator for CQRS operations."""
    
    def __init__(self):
        self.command_bus = CommandBus()
        self.query_bus = QueryBus()
        self.event_bus = EventBus()
    
    async def send_command(self, command: Command) -> CommandResult:
        """Send command through command bus."""
        return await self.command_bus.send(command)
    
    async def send_query(self, query: Query) -> QueryResult:
        """Send query through query bus."""
        return await self.query_bus.send(query)
    
    async def publish_event(self, event: Event):
        """Publish event through event bus."""
        await self.event_bus.publish(event)
    
    def register_command_handler(self, command_type: Type[Command], handler: CommandHandler):
        """Register command handler."""
        self.command_bus.register_handler(command_type, handler)
    
    def register_query_handler(self, query_type: Type[Query], handler: QueryHandler):
        """Register query handler."""
        self.query_bus.register_handler(query_type, handler)
    
    def register_event_handler(self, handler: EventHandler):
        """Register event handler."""
        self.event_bus.register_handler(handler)
    
    def set_cache_manager(self, cache_manager):
        """Set cache manager for query bus."""
        self.query_bus.cache_manager = cache_manager


# Middleware

class LoggingMiddleware:
    """Logging middleware for CQRS operations."""
    
    async def process(self, request: Union[Command, Query]):
        """Process request with logging."""
        logger.info(f"Processing {type(request).__name__} with ID {getattr(request, 'command_id', getattr(request, 'query_id', 'unknown'))}")
        return request


class ValidationMiddleware:
    """Validation middleware for CQRS operations."""
    
    async def process(self, request: Union[Command, Query]):
        """Process request with validation."""
        # Perform additional validation
        if hasattr(request, 'validate_command') and not request.validate_command():
            raise ValueError(f"Validation failed for {type(request).__name__}")
        
        return request


class PerformanceMiddleware:
    """Performance monitoring middleware."""
    
    def __init__(self):
        self.metrics = {}
    
    async def process(self, request: Union[Command, Query]):
        """Process request with performance monitoring."""
        request_type = type(request).__name__
        
        if request_type not in self.metrics:
            self.metrics[request_type] = {
                "count": 0,
                "total_time": 0,
                "avg_time": 0
            }
        
        self.metrics[request_type]["count"] += 1
        
        return request


# Global CQRS instance
_default_cqrs: Optional[CQRSMediator] = None


def get_cqrs_mediator() -> CQRSMediator:
    """Get default CQRS mediator instance."""
    global _default_cqrs
    if _default_cqrs is None:
        _default_cqrs = CQRSMediator()
        
        # Add default middleware
        _default_cqrs.command_bus.add_middleware(LoggingMiddleware())
        _default_cqrs.command_bus.add_middleware(ValidationMiddleware())
        _default_cqrs.query_bus.add_middleware(LoggingMiddleware())
        
    return _default_cqrs


def set_cqrs_mediator(mediator: CQRSMediator):
    """Set default CQRS mediator instance."""
    global _default_cqrs
    _default_cqrs = mediator