"""
Order Fulfillment Domain Entity
==============================

Comprehensive order fulfillment domain model with multi-channel support,
automated workflows, real-time tracking, and performance optimization.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, validator, root_validator, model_validator
from pydantic.types import PositiveInt, PositiveFloat, NonNegativeFloat, NonNegativeInt


class FulfillmentStatus(str, Enum):
    """Order fulfillment status."""
    PENDING = "pending"
    CONFIRMED = "confirmed"
    IN_PROGRESS = "in_progress"
    PICKING = "picking"
    PICKED = "picked"
    PACKING = "packing"
    PACKED = "packed"
    READY_TO_SHIP = "ready_to_ship"
    SHIPPED = "shipped"
    IN_TRANSIT = "in_transit"
    OUT_FOR_DELIVERY = "out_for_delivery"
    DELIVERED = "delivered"
    EXCEPTION = "exception"
    CANCELLED = "cancelled"
    RETURNED = "returned"
    REFUNDED = "refunded"


class ShippingMethod(str, Enum):
    """Shipping method options."""
    STANDARD = "standard"
    EXPEDITED = "expedited"
    OVERNIGHT = "overnight"
    TWO_DAY = "two_day"
    SAME_DAY = "same_day"
    PICKUP = "pickup"
    WHITE_GLOVE = "white_glove"
    FREIGHT = "freight"
    DIGITAL_DELIVERY = "digital_delivery"


class PackagingType(str, Enum):
    """Package types."""
    ENVELOPE = "envelope"
    BOX_SMALL = "box_small"
    BOX_MEDIUM = "box_medium"
    BOX_LARGE = "box_large"
    BOX_EXTRA_LARGE = "box_extra_large"
    TUBE = "tube"
    PAK = "pak"
    CUSTOM = "custom"
    PALLET = "pallet"


class CarrierType(str, Enum):
    """Shipping carriers."""
    FEDEX = "fedex"
    UPS = "ups"
    USPS = "usps"
    DHL = "dhl"
    AMAZON = "amazon"
    LOCAL_COURIER = "local_courier"
    PICKUP = "pickup"
    CUSTOM = "custom"


class FulfillmentChannel(str, Enum):
    """Fulfillment channels."""
    WAREHOUSE = "warehouse"
    STORE = "store"
    DROP_SHIP = "drop_ship"
    THIRD_PARTY = "third_party"
    MARKETPLACE = "marketplace"
    DIGITAL = "digital"


class Priority(str, Enum):
    """Order priority levels."""
    LOW = "low"
    NORMAL = "normal"
    HIGH = "high"
    URGENT = "urgent"
    CRITICAL = "critical"


@dataclass
class ShippingAddress:
    """Shipping address information."""
    name: str
    company: Optional[str] = None
    address_line_1: str = ""
    address_line_2: Optional[str] = None
    city: str = ""
    state_province: str = ""
    postal_code: str = ""
    country: str = ""
    phone: Optional[str] = None
    email: Optional[str] = None
    is_residential: bool = True
    special_instructions: Optional[str] = None
    
    def to_string(self) -> str:
        """Convert address to formatted string."""
        lines = [self.name]
        if self.company:
            lines.append(self.company)
        lines.append(self.address_line_1)
        if self.address_line_2:
            lines.append(self.address_line_2)
        lines.append(f"{self.city}, {self.state_province} {self.postal_code}")
        lines.append(self.country)
        return "\n".join(lines)


class TrackingEvent(BaseModel):
    """Package tracking event."""
    event_id: UUID = Field(default_factory=uuid4)
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    status: str = Field(..., min_length=1)
    location: Optional[str] = None
    description: str = Field(..., min_length=1)
    carrier_code: Optional[str] = None
    exception_code: Optional[str] = None
    next_expected_event: Optional[str] = None
    estimated_delivery: Optional[datetime] = None
    
    # Geolocation
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    
    # Additional details
    facility: Optional[str] = None
    vehicle_id: Optional[str] = None
    driver_name: Optional[str] = None
    signature_required: bool = False
    photo_proof: Optional[str] = None


class FulfillmentLineItem(BaseModel):
    """Individual line item in fulfillment."""
    line_item_id: UUID = Field(default_factory=uuid4)
    order_line_item_id: str = Field(..., min_length=1)
    
    # Product information
    product_id: str = Field(..., min_length=1)
    sku: str = Field(..., min_length=1)
    product_name: str = Field(..., min_length=1)
    variant_id: Optional[str] = None
    
    # Quantities
    ordered_quantity: PositiveInt
    fulfilled_quantity: NonNegativeInt = 0
    cancelled_quantity: NonNegativeInt = 0
    returned_quantity: NonNegativeInt = 0
    
    # Inventory allocation
    allocated_from_location: Optional[str] = None
    batch_id: Optional[str] = None
    lot_number: Optional[str] = None
    serial_numbers: List[str] = Field(default_factory=list)
    expiration_date: Optional[date] = None
    
    # Fulfillment details
    pick_location: Optional[str] = None
    picked_at: Optional[datetime] = None
    picked_by: Optional[str] = None
    packed_at: Optional[datetime] = None
    packed_by: Optional[str] = None
    
    # Special handling
    requires_special_handling: bool = False
    handling_instructions: Optional[str] = None
    is_hazardous: bool = False
    temperature_controlled: bool = False
    
    # Quality control
    quality_checked: bool = False
    quality_check_passed: bool = True
    quality_notes: Optional[str] = None
    
    @validator('fulfilled_quantity')
    def validate_fulfilled_quantity(cls, v, values):
        """Ensure fulfilled quantity doesn't exceed ordered quantity."""
        ordered = values.get('ordered_quantity', 0)
        if v > ordered:
            raise ValueError("Fulfilled quantity cannot exceed ordered quantity")
        return v
    
    @property
    def remaining_quantity(self) -> int:
        """Calculate remaining quantity to fulfill."""
        return self.ordered_quantity - self.fulfilled_quantity - self.cancelled_quantity
    
    @property
    def is_fully_fulfilled(self) -> bool:
        """Check if line item is fully fulfilled."""
        return self.remaining_quantity <= 0


class PackageInfo(BaseModel):
    """Package information."""
    package_id: UUID = Field(default_factory=uuid4)
    tracking_number: Optional[str] = None
    
    # Package details
    packaging_type: PackagingType = PackagingType.BOX_MEDIUM
    weight_lbs: NonNegativeFloat = Field(default=0.0)
    length_inches: NonNegativeFloat = Field(default=0.0)
    width_inches: NonNegativeFloat = Field(default=0.0)
    height_inches: NonNegativeFloat = Field(default=0.0)
    
    # Shipping
    carrier: CarrierType
    service_type: ShippingMethod
    shipping_cost: NonNegativeFloat = Field(default=0.0)
    insurance_value: NonNegativeFloat = Field(default=0.0)
    
    # Contents
    line_items: List[FulfillmentLineItem] = Field(default_factory=list)
    
    # Dates
    ship_date: Optional[datetime] = None
    expected_delivery_date: Optional[datetime] = None
    actual_delivery_date: Optional[datetime] = None
    
    # Tracking
    tracking_events: List[TrackingEvent] = Field(default_factory=list)
    current_status: Optional[str] = None
    current_location: Optional[str] = None
    
    # Labels and documentation
    shipping_label_url: Optional[str] = None
    commercial_invoice_url: Optional[str] = None
    packing_slip_url: Optional[str] = None
    
    def add_tracking_event(self, event: TrackingEvent):
        """Add tracking event."""
        self.tracking_events.append(event)
        self.current_status = event.status
        self.current_location = event.location
        if event.estimated_delivery:
            self.expected_delivery_date = event.estimated_delivery
    
    @property
    def total_weight(self) -> float:
        """Calculate total package weight."""
        return float(self.weight_lbs)
    
    @property
    def dimensional_weight(self) -> float:
        """Calculate dimensional weight."""
        if self.length_inches > 0 and self.width_inches > 0 and self.height_inches > 0:
            # Standard dimensional weight calculation (length x width x height / 139)
            return (self.length_inches * self.width_inches * self.height_inches) / 139.0
        return 0.0
    
    @property
    def billable_weight(self) -> float:
        """Calculate billable weight (greater of actual or dimensional)."""
        return max(self.total_weight, self.dimensional_weight)


class FulfillmentWorkflowStep(BaseModel):
    """Workflow step in fulfillment process."""
    step_id: UUID = Field(default_factory=uuid4)
    step_name: str = Field(..., min_length=1)
    step_order: PositiveInt
    
    # Status
    status: str = Field(default="pending")
    assigned_to: Optional[str] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    
    # Time tracking
    estimated_duration_minutes: NonNegativeInt = 0
    actual_duration_minutes: Optional[int] = None
    
    # Dependencies
    depends_on_steps: List[str] = Field(default_factory=list)
    blocking_steps: List[str] = Field(default_factory=list)
    
    # Automation
    is_automated: bool = False
    automation_rule: Optional[str] = None
    requires_manual_approval: bool = False
    
    # Notes and instructions
    instructions: Optional[str] = None
    notes: Optional[str] = None
    
    def start_step(self, assigned_user: Optional[str] = None):
        """Start the workflow step."""
        self.status = "in_progress"
        self.started_at = datetime.utcnow()
        if assigned_user:
            self.assigned_to = assigned_user
    
    def complete_step(self, notes: Optional[str] = None):
        """Complete the workflow step."""
        self.status = "completed"
        self.completed_at = datetime.utcnow()
        if self.started_at:
            duration = self.completed_at - self.started_at
            self.actual_duration_minutes = int(duration.total_seconds() / 60)
        if notes:
            self.notes = notes
    
    @property
    def is_completed(self) -> bool:
        """Check if step is completed."""
        return self.status == "completed"
    
    @property
    def is_overdue(self) -> bool:
        """Check if step is overdue."""
        if not self.started_at or self.estimated_duration_minutes == 0:
            return False
        
        if self.is_completed:
            return False
        
        expected_completion = self.started_at + timedelta(minutes=self.estimated_duration_minutes)
        return datetime.utcnow() > expected_completion


class FulfillmentException(BaseModel):
    """Fulfillment exception or issue."""
    exception_id: UUID = Field(default_factory=uuid4)
    exception_type: str = Field(..., min_length=1)  # inventory_shortage, damaged_goods, etc.
    severity: str = Field(default="medium")  # low, medium, high, critical
    
    # Description
    title: str = Field(..., min_length=1)
    description: str = Field(..., min_length=1)
    affected_items: List[str] = Field(default_factory=list)  # SKUs or line item IDs
    
    # Resolution
    status: str = Field(default="open")  # open, in_progress, resolved, escalated
    assigned_to: Optional[str] = None
    resolution_notes: Optional[str] = None
    
    # Dates
    created_at: datetime = Field(default_factory=datetime.utcnow)
    resolved_at: Optional[datetime] = None
    
    # Impact
    estimated_delay_hours: NonNegativeInt = 0
    cost_impact: NonNegativeFloat = Field(default=0.0)
    customer_impact: Optional[str] = None
    
    def resolve(self, resolution_notes: str, resolved_by: Optional[str] = None):
        """Mark exception as resolved."""
        self.status = "resolved"
        self.resolution_notes = resolution_notes
        self.resolved_at = datetime.utcnow()
        if resolved_by:
            self.assigned_to = resolved_by


class FulfillmentOrder(BaseModel):
    """
    Comprehensive order fulfillment entity with workflow management,
    real-time tracking, and performance optimization.
    """
    
    # Core identifiers
    fulfillment_id: UUID = Field(default_factory=uuid4)
    order_id: str = Field(..., min_length=1)
    order_number: str = Field(..., min_length=1)
    
    # Customer information
    customer_id: str = Field(..., min_length=1)
    customer_name: str = Field(..., min_length=1)
    customer_email: Optional[str] = None
    
    # Fulfillment details
    status: FulfillmentStatus = FulfillmentStatus.PENDING
    priority: Priority = Priority.NORMAL
    channel: FulfillmentChannel = FulfillmentChannel.WAREHOUSE
    
    # Location and assignment
    fulfillment_center_id: str = Field(..., min_length=1)
    fulfillment_center_name: Optional[str] = None
    assigned_team: Optional[str] = None
    assigned_picker: Optional[str] = None
    assigned_packer: Optional[str] = None
    
    # Dates and timing
    created_at: datetime = Field(default_factory=datetime.utcnow)
    promised_ship_date: Optional[datetime] = None
    promised_delivery_date: Optional[datetime] = None
    actual_ship_date: Optional[datetime] = None
    actual_delivery_date: Optional[datetime] = None
    
    # Items and packages
    line_items: List[FulfillmentLineItem] = Field(default_factory=list)
    packages: List[PackageInfo] = Field(default_factory=list)
    
    # Shipping information
    shipping_address: Optional[ShippingAddress] = None
    shipping_method: ShippingMethod = ShippingMethod.STANDARD
    shipping_cost: NonNegativeFloat = Field(default=0.0)
    
    # Workflow management
    workflow_steps: List[FulfillmentWorkflowStep] = Field(default_factory=list)
    current_step: Optional[str] = None
    
    # Exception handling
    exceptions: List[FulfillmentException] = Field(default_factory=list)
    holds: List[str] = Field(default_factory=list)  # Reasons for holds
    
    # Performance metrics
    pick_start_time: Optional[datetime] = None
    pick_end_time: Optional[datetime] = None
    pack_start_time: Optional[datetime] = None
    pack_end_time: Optional[datetime] = None
    
    # Quality and compliance
    quality_check_required: bool = False
    quality_check_completed: bool = False
    quality_issues: List[str] = Field(default_factory=list)
    
    # Cost tracking
    fulfillment_cost: NonNegativeFloat = Field(default=0.0)
    labor_cost: NonNegativeFloat = Field(default=0.0)
    packaging_cost: NonNegativeFloat = Field(default=0.0)
    
    # Special handling
    gift_wrap_required: bool = False
    gift_message: Optional[str] = None
    special_instructions: Optional[str] = None
    
    # Notifications
    customer_notifications_sent: List[str] = Field(default_factory=list)
    internal_notifications_sent: List[str] = Field(default_factory=list)
    
    # Audit information
    created_by: Optional[str] = None
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    updated_by: Optional[str] = None
    
    # External references
    external_order_id: Optional[str] = None
    marketplace_order_id: Optional[str] = None
    
    @validator('line_items')
    def validate_line_items(cls, v):
        """Ensure at least one line item exists."""
        if not v:
            raise ValueError("At least one line item is required")
        return v
    
    @model_validator(mode="before")
    @classmethod
    def validate_dates(cls, values):
        """Validate date consistency."""
        promised_ship = values.get('promised_ship_date')
        promised_delivery = values.get('promised_delivery_date')
        
        if promised_ship and promised_delivery:
            if promised_ship >= promised_delivery:
                raise ValueError("Promised ship date must be before promised delivery date")
        
        return values
    
    def add_line_item(self, line_item: FulfillmentLineItem):
        """Add line item to fulfillment."""
        self.line_items.append(line_item)
        self.updated_at = datetime.utcnow()
    
    def add_package(self, package: PackageInfo):
        """Add package to fulfillment."""
        self.packages.append(package)
        self.updated_at = datetime.utcnow()
        
        # Update status if this is first package
        if len(self.packages) == 1 and self.status in [FulfillmentStatus.PACKED, FulfillmentStatus.READY_TO_SHIP]:
            self.status = FulfillmentStatus.SHIPPED
            self.actual_ship_date = datetime.utcnow()
    
    def add_exception(self, exception: FulfillmentException):
        """Add fulfillment exception."""
        self.exceptions.append(exception)
        if self.status not in [FulfillmentStatus.EXCEPTION, FulfillmentStatus.CANCELLED]:
            self.status = FulfillmentStatus.EXCEPTION
        self.updated_at = datetime.utcnow()
    
    def update_status(self, new_status: FulfillmentStatus, notes: Optional[str] = None):
        """Update fulfillment status."""
        old_status = self.status
        self.status = new_status
        self.updated_at = datetime.utcnow()
        
        # Update workflow step if applicable
        if self.current_step:
            current_workflow_step = self.get_current_workflow_step()
            if current_workflow_step and not current_workflow_step.is_completed:
                current_workflow_step.complete_step(notes)
        
        # Update timing based on status
        now = datetime.utcnow()
        if new_status == FulfillmentStatus.PICKING and not self.pick_start_time:
            self.pick_start_time = now
        elif new_status == FulfillmentStatus.PICKED and self.pick_start_time and not self.pick_end_time:
            self.pick_end_time = now
        elif new_status == FulfillmentStatus.PACKING and not self.pack_start_time:
            self.pack_start_time = now
        elif new_status == FulfillmentStatus.PACKED and self.pack_start_time and not self.pack_end_time:
            self.pack_end_time = now
        elif new_status == FulfillmentStatus.SHIPPED and not self.actual_ship_date:
            self.actual_ship_date = now
        elif new_status == FulfillmentStatus.DELIVERED and not self.actual_delivery_date:
            self.actual_delivery_date = now
    
    def get_current_workflow_step(self) -> Optional[FulfillmentWorkflowStep]:
        """Get current workflow step."""
        if not self.current_step:
            return None
        
        for step in self.workflow_steps:
            if step.step_name == self.current_step:
                return step
        
        return None
    
    def advance_workflow(self):
        """Advance to next workflow step."""
        if not self.workflow_steps:
            return
        
        current_step = self.get_current_workflow_step()
        if not current_step or not current_step.is_completed:
            return
        
        # Find next uncompleted step
        sorted_steps = sorted(self.workflow_steps, key=lambda x: x.step_order)
        current_index = next(i for i, step in enumerate(sorted_steps) if step.step_name == self.current_step)
        
        for i in range(current_index + 1, len(sorted_steps)):
            next_step = sorted_steps[i]
            if not next_step.is_completed:
                # Check if dependencies are met
                dependencies_met = all(
                    any(s.step_name == dep and s.is_completed for s in self.workflow_steps)
                    for dep in next_step.depends_on_steps
                )
                
                if dependencies_met:
                    self.current_step = next_step.step_name
                    next_step.start_step()
                    break
    
    def calculate_fulfillment_metrics(self) -> Dict[str, Any]:
        """Calculate fulfillment performance metrics."""
        metrics = {}
        
        # Time metrics
        if self.pick_start_time and self.pick_end_time:
            pick_duration = (self.pick_end_time - self.pick_start_time).total_seconds() / 60
            metrics['pick_time_minutes'] = pick_duration
        
        if self.pack_start_time and self.pack_end_time:
            pack_duration = (self.pack_end_time - self.pack_start_time).total_seconds() / 60
            metrics['pack_time_minutes'] = pack_duration
        
        # Order cycle time
        if self.actual_ship_date:
            cycle_time = (self.actual_ship_date - self.created_at).total_seconds() / 3600
            metrics['order_cycle_time_hours'] = cycle_time
        
        # Delivery performance
        if self.promised_delivery_date and self.actual_delivery_date:
            delivery_variance = (self.actual_delivery_date - self.promised_delivery_date).days
            metrics['delivery_variance_days'] = delivery_variance
            metrics['on_time_delivery'] = delivery_variance <= 0
        
        # Quality metrics
        total_items = sum(item.ordered_quantity for item in self.line_items)
        quality_issues_count = len(self.quality_issues)
        metrics['quality_issue_rate'] = quality_issues_count / max(total_items, 1)
        
        # Exception rate
        metrics['exception_count'] = len(self.exceptions)
        metrics['has_exceptions'] = len(self.exceptions) > 0
        
        # Cost efficiency
        if total_items > 0:
            metrics['fulfillment_cost_per_item'] = float(self.fulfillment_cost) / total_items
            metrics['labor_cost_per_item'] = float(self.labor_cost) / total_items
        
        return metrics
    
    def is_overdue(self) -> bool:
        """Check if fulfillment is overdue."""
        if not self.promised_ship_date:
            return False
        
        if self.status in [FulfillmentStatus.SHIPPED, FulfillmentStatus.DELIVERED]:
            return False
        
        return datetime.utcnow() > self.promised_ship_date
    
    def get_estimated_ship_date(self) -> Optional[datetime]:
        """Calculate estimated ship date based on current progress."""
        if self.actual_ship_date:
            return self.actual_ship_date
        
        # Simple estimation based on remaining workflow steps
        remaining_steps = [
            step for step in self.workflow_steps 
            if not step.is_completed and step.estimated_duration_minutes > 0
        ]
        
        if not remaining_steps:
            return datetime.utcnow()
        
        total_remaining_minutes = sum(step.estimated_duration_minutes for step in remaining_steps)
        return datetime.utcnow() + timedelta(minutes=total_remaining_minutes)
    
    def calculate_fill_rate(self) -> float:
        """Calculate order fill rate."""
        if not self.line_items:
            return 1.0
        
        total_ordered = sum(item.ordered_quantity for item in self.line_items)
        total_fulfilled = sum(item.fulfilled_quantity for item in self.line_items)
        
        if total_ordered == 0:
            return 1.0
        
        return total_fulfilled / total_ordered
    
    def send_customer_notification(self, notification_type: str, message: str):
        """Send notification to customer."""
        # This would integrate with notification service
        notification_key = f"{notification_type}_{datetime.utcnow().isoformat()}"
        self.customer_notifications_sent.append(notification_key)
        self.updated_at = datetime.utcnow()
    
    def to_analytics_dict(self) -> Dict[str, Any]:
        """Convert to dictionary optimized for analytics."""
        metrics = self.calculate_fulfillment_metrics()
        
        return {
            'fulfillment_id': str(self.fulfillment_id),
            'order_id': self.order_id,
            'order_number': self.order_number,
            'customer_id': self.customer_id,
            
            # Status and timing
            'status': self.status.value,
            'priority': self.priority.value,
            'channel': self.channel.value,
            'created_at': self.created_at.isoformat(),
            'promised_ship_date': self.promised_ship_date.isoformat() if self.promised_ship_date else None,
            'actual_ship_date': self.actual_ship_date.isoformat() if self.actual_ship_date else None,
            'promised_delivery_date': self.promised_delivery_date.isoformat() if self.promised_delivery_date else None,
            'actual_delivery_date': self.actual_delivery_date.isoformat() if self.actual_delivery_date else None,
            
            # Fulfillment center
            'fulfillment_center_id': self.fulfillment_center_id,
            'fulfillment_center_name': self.fulfillment_center_name,
            
            # Quantities
            'line_item_count': len(self.line_items),
            'total_ordered_items': sum(item.ordered_quantity for item in self.line_items),
            'total_fulfilled_items': sum(item.fulfilled_quantity for item in self.line_items),
            'package_count': len(self.packages),
            
            # Performance metrics
            'fill_rate': self.calculate_fill_rate(),
            'is_overdue': self.is_overdue(),
            'exception_count': len(self.exceptions),
            'has_exceptions': len(self.exceptions) > 0,
            'quality_check_required': self.quality_check_required,
            'quality_issues_count': len(self.quality_issues),
            
            # Costs
            'fulfillment_cost': float(self.fulfillment_cost),
            'labor_cost': float(self.labor_cost),
            'packaging_cost': float(self.packaging_cost),
            'shipping_cost': float(self.shipping_cost),
            'total_cost': float(self.fulfillment_cost + self.labor_cost + self.packaging_cost + self.shipping_cost),
            
            # Shipping
            'shipping_method': self.shipping_method.value,
            'shipping_country': self.shipping_address.country if self.shipping_address else None,
            'shipping_state': self.shipping_address.state_province if self.shipping_address else None,
            'is_residential': self.shipping_address.is_residential if self.shipping_address else None,
            
            # Special handling
            'gift_wrap_required': self.gift_wrap_required,
            'has_special_instructions': bool(self.special_instructions),
            
            # Workflow
            'workflow_step_count': len(self.workflow_steps),
            'current_step': self.current_step,
            'completed_steps': sum(1 for step in self.workflow_steps if step.is_completed),
            
            # Additional metrics from calculation
            **metrics
        }
    
    class Config:
        """Pydantic configuration."""
        use_enum_values = True
        validate_assignment = True
        arbitrary_types_allowed = True
        json_encoders = {
            datetime: lambda v: v.isoformat(),
            date: lambda v: v.isoformat(),
            Decimal: lambda v: float(v),
            UUID: lambda v: str(v)
        }


class FulfillmentAggregate(BaseModel):
    """
    Fulfillment performance aggregate for analytics and reporting.
    """
    
    # Aggregation identifiers
    aggregate_id: UUID = Field(default_factory=uuid4)
    aggregation_type: str = Field(..., description="fulfillment_center, channel, priority, etc.")
    aggregation_key: str = Field(..., description="The key being aggregated by")
    
    # Time period
    period_start: datetime
    period_end: datetime
    period_type: str = Field(default="daily")  # daily, weekly, monthly
    
    # Volume metrics
    total_orders: NonNegativeInt = 0
    total_line_items: NonNegativeInt = 0
    total_packages: NonNegativeInt = 0
    
    # Performance metrics
    on_time_ship_rate: NonNegativeFloat = Field(default=0.0, ge=0.0, le=1.0)
    on_time_delivery_rate: NonNegativeFloat = Field(default=0.0, ge=0.0, le=1.0)
    average_fill_rate: NonNegativeFloat = Field(default=0.0, ge=0.0, le=1.0)
    
    # Time metrics
    average_pick_time_minutes: NonNegativeFloat = 0.0
    average_pack_time_minutes: NonNegativeFloat = 0.0
    average_cycle_time_hours: NonNegativeFloat = 0.0
    
    # Quality metrics
    exception_rate: NonNegativeFloat = Field(default=0.0, ge=0.0, le=1.0)
    quality_issue_rate: NonNegativeFloat = Field(default=0.0, ge=0.0, le=1.0)
    
    # Cost metrics
    total_fulfillment_cost: NonNegativeFloat = 0.0
    average_cost_per_order: NonNegativeFloat = 0.0
    average_cost_per_item: NonNegativeFloat = 0.0
    
    # Status breakdown
    status_breakdown: Dict[str, int] = Field(default_factory=dict)
    priority_breakdown: Dict[str, int] = Field(default_factory=dict)
    channel_breakdown: Dict[str, int] = Field(default_factory=dict)
    
    # Metadata
    calculated_at: datetime = Field(default_factory=datetime.utcnow)
    data_source: str = Field(default="system")


# Factory functions

def create_fulfillment_order(
    order_id: str,
    order_number: str,
    customer_id: str,
    customer_name: str,
    fulfillment_center_id: str,
    line_items: List[Dict[str, Any]],
    **kwargs
) -> FulfillmentOrder:
    """Factory function to create a fulfillment order."""
    
    # Convert line items
    fulfillment_line_items = []
    for item_data in line_items:
        line_item = FulfillmentLineItem(**item_data)
        fulfillment_line_items.append(line_item)
    
    fulfillment_data = {
        'order_id': order_id,
        'order_number': order_number,
        'customer_id': customer_id,
        'customer_name': customer_name,
        'fulfillment_center_id': fulfillment_center_id,
        'line_items': fulfillment_line_items,
        **kwargs
    }
    
    return FulfillmentOrder(**fulfillment_data)


def create_standard_workflow() -> List[FulfillmentWorkflowStep]:
    """Create standard fulfillment workflow steps."""
    
    return [
        FulfillmentWorkflowStep(
            step_name="order_validation",
            step_order=1,
            estimated_duration_minutes=5,
            instructions="Validate order details and inventory availability"
        ),
        FulfillmentWorkflowStep(
            step_name="inventory_allocation",
            step_order=2,
            estimated_duration_minutes=10,
            depends_on_steps=["order_validation"],
            instructions="Allocate inventory from available stock"
        ),
        FulfillmentWorkflowStep(
            step_name="picking",
            step_order=3,
            estimated_duration_minutes=15,
            depends_on_steps=["inventory_allocation"],
            instructions="Pick items from warehouse locations"
        ),
        FulfillmentWorkflowStep(
            step_name="quality_check",
            step_order=4,
            estimated_duration_minutes=5,
            depends_on_steps=["picking"],
            instructions="Perform quality inspection of picked items"
        ),
        FulfillmentWorkflowStep(
            step_name="packing",
            step_order=5,
            estimated_duration_minutes=10,
            depends_on_steps=["quality_check"],
            instructions="Pack items for shipping"
        ),
        FulfillmentWorkflowStep(
            step_name="shipping_label",
            step_order=6,
            estimated_duration_minutes=3,
            depends_on_steps=["packing"],
            is_automated=True,
            instructions="Generate shipping labels and documentation"
        ),
        FulfillmentWorkflowStep(
            step_name="handoff_to_carrier",
            step_order=7,
            estimated_duration_minutes=5,
            depends_on_steps=["shipping_label"],
            instructions="Hand off packages to shipping carrier"
        )
    ]


def create_sample_fulfillment() -> FulfillmentOrder:
    """Create sample fulfillment order for testing."""
    
    line_items = [
        {
            'order_line_item_id': 'LINE-001',
            'product_id': 'PROD-001',
            'sku': 'SKU-001', 
            'product_name': 'Wireless Bluetooth Headphones',
            'ordered_quantity': 2,
            'fulfilled_quantity': 0
        },
        {
            'order_line_item_id': 'LINE-002',
            'product_id': 'PROD-002',
            'sku': 'SKU-002',
            'product_name': 'Organic Cotton T-Shirt',
            'ordered_quantity': 1,
            'fulfilled_quantity': 0
        }
    ]
    
    shipping_address = ShippingAddress(
        name="John Doe",
        address_line_1="123 Main St",
        city="New York",
        state_province="NY",
        postal_code="10001",
        country="US",
        phone="555-123-4567",
        is_residential=True
    )
    
    fulfillment = create_fulfillment_order(
        order_id="ORD-12345",
        order_number="ORDER-2024-001",
        customer_id="CUST-001",
        customer_name="John Doe",
        fulfillment_center_id="FC-NYC-01",
        line_items=line_items,
        shipping_address=shipping_address,
        shipping_method=ShippingMethod.STANDARD,
        priority=Priority.NORMAL,
        workflow_steps=create_standard_workflow(),
        promised_ship_date=datetime.utcnow() + timedelta(hours=24),
        promised_delivery_date=datetime.utcnow() + timedelta(days=5)
    )
    
    # Start first workflow step
    if fulfillment.workflow_steps:
        fulfillment.current_step = fulfillment.workflow_steps[0].step_name
        fulfillment.workflow_steps[0].start_step()
    
    return fulfillment