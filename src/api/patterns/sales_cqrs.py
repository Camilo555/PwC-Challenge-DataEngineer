"""
Sales Domain CQRS Implementation
Concrete commands, queries, and handlers for the sales domain
"""
from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional
from decimal import Decimal

from pydantic import Field, validator

from .cqrs import Command, Query, CommandHandler, QueryHandler, EventHandler, Event, CommandResult, QueryResult, CommandStatus, QueryType
from domain.entities.sale import Sale
from domain.entities.customer import Customer
from domain.entities.product import Product
from core.logging import get_logger

logger = get_logger(__name__)


# Sales Commands

class CreateSaleCommand(Command):
    """Command to create a new sale."""
    customer_id: str
    products: List[Dict[str, Any]]
    total_amount: Decimal
    sale_date: datetime
    store_location: Optional[str] = None
    sales_rep_id: Optional[str] = None
    
    def validate_command(self) -> bool:
        """Validate create sale command."""
        if not self.customer_id:
            return False
        if not self.products or len(self.products) == 0:
            return False
        if self.total_amount <= 0:
            return False
        return True


class UpdateSaleCommand(Command):
    """Command to update an existing sale."""
    sale_id: str
    update_data: Dict[str, Any]
    
    def validate_command(self) -> bool:
        """Validate update sale command."""
        if not self.sale_id:
            return False
        if not self.update_data:
            return False
        return True


class DeleteSaleCommand(Command):
    """Command to delete a sale."""
    sale_id: str
    reason: Optional[str] = None
    
    def validate_command(self) -> bool:
        """Validate delete sale command."""
        return bool(self.sale_id)


class ProcessBulkSalesCommand(Command):
    """Command to process multiple sales in bulk."""
    sales_data: List[Dict[str, Any]]
    batch_id: str
    
    def validate_command(self) -> bool:
        """Validate bulk sales command."""
        if not self.sales_data or len(self.sales_data) == 0:
            return False
        if not self.batch_id:
            return False
        return True


# Sales Queries

class GetSaleQuery(Query):
    """Query to get a single sale by ID."""
    sale_id: str
    include_details: bool = True
    
    def __init__(self, **data):
        super().__init__(**data)
        self.query_type = QueryType.SIMPLE


class GetSalesByCustomerQuery(Query):
    """Query to get sales by customer."""
    customer_id: str
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    limit: int = 100
    offset: int = 0
    
    def __init__(self, **data):
        super().__init__(**data)
        self.query_type = QueryType.SIMPLE


class GetSalesAnalyticsQuery(Query):
    """Query for sales analytics."""
    start_date: datetime
    end_date: datetime
    group_by: str = "day"  # day, week, month
    metrics: List[str] = Field(default_factory=lambda: ["total_sales", "order_count", "avg_order_value"])
    filters: Dict[str, Any] = Field(default_factory=dict)
    
    def __init__(self, **data):
        super().__init__(**data)
        self.query_type = QueryType.ANALYTICAL


class GetTopProductsQuery(Query):
    """Query to get top-selling products."""
    start_date: datetime
    end_date: datetime
    limit: int = 10
    metric: str = "quantity"  # quantity, revenue
    
    def __init__(self, **data):
        super().__init__(**data)
        self.query_type = QueryType.AGGREGATION


class GetSalesReportQuery(Query):
    """Query for comprehensive sales report."""
    start_date: datetime
    end_date: datetime
    report_type: str = "summary"  # summary, detailed, comparative
    filters: Dict[str, Any] = Field(default_factory=dict)
    
    def __init__(self, **data):
        super().__init__(**data)
        self.query_type = QueryType.REPORTING


# Sales Events

class SaleCreatedEvent(Event):
    """Event fired when a sale is created."""
    def __init__(self, sale_id: str, customer_id: str, total_amount: float, **kwargs):
        super().__init__(
            event_type="SaleCreated",
            aggregate_id=sale_id,
            aggregate_type="Sale",
            payload={
                "sale_id": sale_id,
                "customer_id": customer_id,
                "total_amount": total_amount,
                **kwargs
            }
        )


class SaleUpdatedEvent(Event):
    """Event fired when a sale is updated."""
    def __init__(self, sale_id: str, updated_fields: Dict[str, Any], **kwargs):
        super().__init__(
            event_type="SaleUpdated",
            aggregate_id=sale_id,
            aggregate_type="Sale",
            payload={
                "sale_id": sale_id,
                "updated_fields": updated_fields,
                **kwargs
            }
        )


class SaleDeletedEvent(Event):
    """Event fired when a sale is deleted."""
    def __init__(self, sale_id: str, reason: Optional[str] = None, **kwargs):
        super().__init__(
            event_type="SaleDeleted",
            aggregate_id=sale_id,
            aggregate_type="Sale",
            payload={
                "sale_id": sale_id,
                "reason": reason,
                **kwargs
            }
        )


# Command Handlers

class CreateSaleCommandHandler(CommandHandler[CreateSaleCommand, str]):
    """Handler for creating sales."""
    
    def __init__(self, sales_repository, event_bus):
        self.sales_repository = sales_repository
        self.event_bus = event_bus
    
    async def handle(self, command: CreateSaleCommand) -> CommandResult:
        """Handle create sale command."""
        try:
            # Create sale entity
            sale_data = {
                "customer_id": command.customer_id,
                "products": command.products,
                "total_amount": float(command.total_amount),
                "sale_date": command.sale_date,
                "store_location": command.store_location,
                "sales_rep_id": command.sales_rep_id
            }
            
            # Save to repository
            sale_id = await self.sales_repository.create(sale_data)
            
            # Create and publish event
            event = SaleCreatedEvent(
                sale_id=sale_id,
                customer_id=command.customer_id,
                total_amount=float(command.total_amount),
                correlation_id=command.correlation_id
            )
            await self.event_bus.publish(event)
            
            return CommandResult(
                command_id=command.command_id,
                status=CommandStatus.COMPLETED,
                result={"sale_id": sale_id},
                events=[event.dict()]
            )
            
        except Exception as e:
            logger.error(f"Error creating sale: {e}")
            return CommandResult(
                command_id=command.command_id,
                status=CommandStatus.FAILED,
                error=str(e)
            )
    
    def can_handle(self, command: Command) -> bool:
        """Check if this handler can handle the command."""
        return isinstance(command, CreateSaleCommand)


class UpdateSaleCommandHandler(CommandHandler[UpdateSaleCommand, str]):
    """Handler for updating sales."""
    
    def __init__(self, sales_repository, event_bus):
        self.sales_repository = sales_repository
        self.event_bus = event_bus
    
    async def handle(self, command: UpdateSaleCommand) -> CommandResult:
        """Handle update sale command."""
        try:
            # Update sale in repository
            updated = await self.sales_repository.update(command.sale_id, command.update_data)
            
            if updated:
                # Create and publish event
                event = SaleUpdatedEvent(
                    sale_id=command.sale_id,
                    updated_fields=command.update_data,
                    correlation_id=command.correlation_id
                )
                await self.event_bus.publish(event)
                
                return CommandResult(
                    command_id=command.command_id,
                    status=CommandStatus.COMPLETED,
                    result={"sale_id": command.sale_id, "updated": True},
                    events=[event.dict()]
                )
            else:
                return CommandResult(
                    command_id=command.command_id,
                    status=CommandStatus.FAILED,
                    error=f"Sale {command.sale_id} not found or could not be updated"
                )
                
        except Exception as e:
            logger.error(f"Error updating sale: {e}")
            return CommandResult(
                command_id=command.command_id,
                status=CommandStatus.FAILED,
                error=str(e)
            )
    
    def can_handle(self, command: Command) -> bool:
        """Check if this handler can handle the command."""
        return isinstance(command, UpdateSaleCommand)


class DeleteSaleCommandHandler(CommandHandler[DeleteSaleCommand, str]):
    """Handler for deleting sales."""
    
    def __init__(self, sales_repository, event_bus):
        self.sales_repository = sales_repository
        self.event_bus = event_bus
    
    async def handle(self, command: DeleteSaleCommand) -> CommandResult:
        """Handle delete sale command."""
        try:
            # Delete sale from repository
            deleted = await self.sales_repository.delete(command.sale_id)
            
            if deleted:
                # Create and publish event
                event = SaleDeletedEvent(
                    sale_id=command.sale_id,
                    reason=command.reason,
                    correlation_id=command.correlation_id
                )
                await self.event_bus.publish(event)
                
                return CommandResult(
                    command_id=command.command_id,
                    status=CommandStatus.COMPLETED,
                    result={"sale_id": command.sale_id, "deleted": True},
                    events=[event.dict()]
                )
            else:
                return CommandResult(
                    command_id=command.command_id,
                    status=CommandStatus.FAILED,
                    error=f"Sale {command.sale_id} not found or could not be deleted"
                )
                
        except Exception as e:
            logger.error(f"Error deleting sale: {e}")
            return CommandResult(
                command_id=command.command_id,
                status=CommandStatus.FAILED,
                error=str(e)
            )
    
    def can_handle(self, command: Command) -> bool:
        """Check if this handler can handle the command."""
        return isinstance(command, DeleteSaleCommand)


# Query Handlers

class GetSaleQueryHandler(QueryHandler[GetSaleQuery, Dict[str, Any]]):
    """Handler for getting a single sale."""
    
    def __init__(self, sales_repository):
        self.sales_repository = sales_repository
    
    async def handle(self, query: GetSaleQuery) -> QueryResult[Dict[str, Any]]:
        """Handle get sale query."""
        try:
            sale = await self.sales_repository.get_by_id(query.sale_id)
            
            if not sale:
                return QueryResult(
                    query_id=query.query_id,
                    data=None,
                    metadata={"error": f"Sale {query.sale_id} not found"}
                )
            
            return QueryResult(
                query_id=query.query_id,
                data=sale,
                metadata={"include_details": query.include_details}
            )
            
        except Exception as e:
            logger.error(f"Error getting sale: {e}")
            return QueryResult(
                query_id=query.query_id,
                data=None,
                metadata={"error": str(e)}
            )
    
    def can_handle(self, query: Query) -> bool:
        """Check if this handler can handle the query."""
        return isinstance(query, GetSaleQuery)


class GetSalesByCustomerQueryHandler(QueryHandler[GetSalesByCustomerQuery, List[Dict[str, Any]]]):
    """Handler for getting sales by customer."""
    
    def __init__(self, sales_repository):
        self.sales_repository = sales_repository
    
    async def handle(self, query: GetSalesByCustomerQuery) -> QueryResult[List[Dict[str, Any]]]:
        """Handle get sales by customer query."""
        try:
            sales = await self.sales_repository.get_by_customer(
                customer_id=query.customer_id,
                start_date=query.start_date,
                end_date=query.end_date,
                limit=query.limit,
                offset=query.offset
            )
            
            return QueryResult(
                query_id=query.query_id,
                data=sales,
                metadata={
                    "customer_id": query.customer_id,
                    "count": len(sales),
                    "limit": query.limit,
                    "offset": query.offset
                }
            )
            
        except Exception as e:
            logger.error(f"Error getting sales by customer: {e}")
            return QueryResult(
                query_id=query.query_id,
                data=[],
                metadata={"error": str(e)}
            )
    
    def can_handle(self, query: Query) -> bool:
        """Check if this handler can handle the query."""
        return isinstance(query, GetSalesByCustomerQuery)


class GetSalesAnalyticsQueryHandler(QueryHandler[GetSalesAnalyticsQuery, Dict[str, Any]]):
    """Handler for sales analytics queries."""
    
    def __init__(self, analytics_service):
        self.analytics_service = analytics_service
    
    async def handle(self, query: GetSalesAnalyticsQuery) -> QueryResult[Dict[str, Any]]:
        """Handle sales analytics query."""
        try:
            analytics = await self.analytics_service.get_sales_analytics(
                start_date=query.start_date,
                end_date=query.end_date,
                group_by=query.group_by,
                metrics=query.metrics,
                filters=query.filters
            )
            
            return QueryResult(
                query_id=query.query_id,
                data=analytics,
                metadata={
                    "period": f"{query.start_date} to {query.end_date}",
                    "group_by": query.group_by,
                    "metrics": query.metrics
                }
            )
            
        except Exception as e:
            logger.error(f"Error getting sales analytics: {e}")
            return QueryResult(
                query_id=query.query_id,
                data={},
                metadata={"error": str(e)}
            )
    
    def can_handle(self, query: Query) -> bool:
        """Check if this handler can handle the query."""
        return isinstance(query, GetSalesAnalyticsQuery)


# Event Handlers

class SaleAuditEventHandler(EventHandler):
    """Handler for auditing sale events."""
    
    def __init__(self, audit_service):
        self.audit_service = audit_service
    
    async def handle(self, event: Event) -> None:
        """Handle sale events for auditing."""
        try:
            audit_record = {
                "event_id": event.event_id,
                "event_type": event.event_type,
                "aggregate_id": event.aggregate_id,
                "aggregate_type": event.aggregate_type,
                "payload": event.payload,
                "timestamp": event.timestamp,
                "user_id": event.payload.get("user_id"),
                "correlation_id": event.correlation_id
            }
            
            await self.audit_service.create_audit_record(audit_record)
            logger.info(f"Audit record created for event {event.event_id}")
            
        except Exception as e:
            logger.error(f"Error creating audit record for event {event.event_id}: {e}")
    
    def can_handle(self, event: Event) -> bool:
        """Check if this handler can handle the event."""
        return event.event_type in ["SaleCreated", "SaleUpdated", "SaleDeleted"]


class SaleNotificationEventHandler(EventHandler):
    """Handler for sending notifications on sale events."""
    
    def __init__(self, notification_service):
        self.notification_service = notification_service
    
    async def handle(self, event: Event) -> None:
        """Handle sale events for notifications."""
        try:
            if event.event_type == "SaleCreated":
                await self._handle_sale_created(event)
            elif event.event_type == "SaleUpdated":
                await self._handle_sale_updated(event)
            elif event.event_type == "SaleDeleted":
                await self._handle_sale_deleted(event)
                
        except Exception as e:
            logger.error(f"Error sending notification for event {event.event_id}: {e}")
    
    async def _handle_sale_created(self, event: Event):
        """Handle sale created notification."""
        payload = event.payload
        total_amount = payload.get("total_amount", 0)
        
        # Send high-value sale notifications
        if total_amount > 1000:  # Configurable threshold
            await self.notification_service.send_notification(
                type="high_value_sale",
                message=f"High value sale created: ${total_amount}",
                metadata=payload
            )
    
    async def _handle_sale_updated(self, event: Event):
        """Handle sale updated notification."""
        # Implement update notifications if needed
        pass
    
    async def _handle_sale_deleted(self, event: Event):
        """Handle sale deleted notification."""
        # Implement delete notifications if needed
        pass
    
    def can_handle(self, event: Event) -> bool:
        """Check if this handler can handle the event."""
        return event.event_type in ["SaleCreated", "SaleUpdated", "SaleDeleted"]


# Sales CQRS Setup Function

async def setup_sales_cqrs(mediator, sales_repository, analytics_service, audit_service, notification_service):
    """Setup sales CQRS handlers."""
    
    # Register command handlers
    mediator.register_command_handler(
        CreateSaleCommand, 
        CreateSaleCommandHandler(sales_repository, mediator.event_bus)
    )
    mediator.register_command_handler(
        UpdateSaleCommand,
        UpdateSaleCommandHandler(sales_repository, mediator.event_bus)
    )
    mediator.register_command_handler(
        DeleteSaleCommand,
        DeleteSaleCommandHandler(sales_repository, mediator.event_bus)
    )
    
    # Register query handlers
    mediator.register_query_handler(
        GetSaleQuery,
        GetSaleQueryHandler(sales_repository)
    )
    mediator.register_query_handler(
        GetSalesByCustomerQuery,
        GetSalesByCustomerQueryHandler(sales_repository)
    )
    mediator.register_query_handler(
        GetSalesAnalyticsQuery,
        GetSalesAnalyticsQueryHandler(analytics_service)
    )
    
    # Register event handlers
    mediator.register_event_handler(SaleAuditEventHandler(audit_service))
    mediator.register_event_handler(SaleNotificationEventHandler(notification_service))
    
    logger.info("Sales CQRS setup completed")