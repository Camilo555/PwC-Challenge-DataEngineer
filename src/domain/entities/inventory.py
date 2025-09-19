"""
Inventory Domain Entity
======================

Comprehensive inventory management domain model with real-time tracking,
automated reordering, multi-location support, and predictive analytics.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, date, timedelta
from decimal import Decimal
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple, Union
from uuid import UUID, uuid4

from pydantic import BaseModel, Field, validator, root_validator
from pyda, model_validatorntic.types import PositiveInt, PositiveFloat, NonNegativeFloat, NonNegativeInt


class InventoryStatus(str, Enum):
    """Inventory status types."""
    IN_STOCK = "in_stock"
    LOW_STOCK = "low_stock"
    OUT_OF_STOCK = "out_of_stock"
    ON_ORDER = "on_order"
    DISCONTINUED = "discontinued"
    RESERVED = "reserved"
    DAMAGED = "damaged"
    EXPIRED = "expired"
    QUARANTINE = "quarantine"


class MovementType(str, Enum):
    """Inventory movement types."""
    INBOUND = "inbound"
    OUTBOUND = "outbound"
    TRANSFER = "transfer"
    ADJUSTMENT = "adjustment"
    RETURN = "return"
    DAMAGE = "damage"
    EXPIRY = "expiry"
    THEFT = "theft"
    PROMOTION = "promotion"
    SAMPLE = "sample"


class ReorderStrategy(str, Enum):
    """Automatic reorder strategies."""
    MIN_MAX = "min_max"
    ECONOMIC_ORDER_QUANTITY = "eoq"
    JUST_IN_TIME = "jit"
    SEASONAL_FORECAST = "seasonal_forecast"
    DEMAND_DRIVEN = "demand_driven"
    ABC_ANALYSIS = "abc_analysis"


class LocationType(str, Enum):
    """Storage location types."""
    WAREHOUSE = "warehouse"
    STORE = "store"
    DISTRIBUTION_CENTER = "distribution_center"
    SUPPLIER = "supplier"
    CUSTOMER = "customer"
    IN_TRANSIT = "in_transit"
    QUARANTINE_ZONE = "quarantine_zone"
    RETURNS_CENTER = "returns_center"


class InventoryValuationMethod(str, Enum):
    """Inventory valuation methods."""
    FIFO = "fifo"  # First In, First Out
    LIFO = "lifo"  # Last In, First Out
    AVERAGE_COST = "average_cost"
    STANDARD_COST = "standard_cost"
    SPECIFIC_IDENTIFICATION = "specific_identification"


@dataclass
class StorageLocation:
    """Physical storage location details."""
    location_id: str
    location_type: LocationType
    warehouse_id: Optional[str] = None
    zone: Optional[str] = None
    aisle: Optional[str] = None
    shelf: Optional[str] = None
    bin: Optional[str] = None
    temperature_controlled: bool = False
    hazmat_approved: bool = False
    security_level: str = "standard"
    capacity_units: Optional[int] = None
    created_at: datetime = field(default_factory=datetime.utcnow)


class InventoryMovement(BaseModel):
    """Inventory movement record."""
    movement_id: UUID = Field(default_factory=uuid4)
    product_id: str = Field(..., min_length=1)
    sku: str = Field(..., min_length=1)
    movement_type: MovementType
    
    # Quantities
    quantity: int = Field(..., description="Movement quantity")
    unit_cost: NonNegativeFloat = Field(default=0.0)
    total_cost: NonNegativeFloat = Field(default=0.0)
    
    # Location information
    from_location: Optional[str] = None
    to_location: Optional[str] = None
    
    # Reference information
    reference_type: Optional[str] = None  # order, transfer, adjustment, etc.
    reference_id: Optional[str] = None
    batch_id: Optional[str] = None
    lot_number: Optional[str] = None
    serial_numbers: List[str] = Field(default_factory=list)
    
    # Audit information
    created_by: str = Field(..., min_length=1)
    created_at: datetime = Field(default_factory=datetime.utcnow)
    notes: Optional[str] = None
    
    @validator('total_cost', pre=True, always=True)
    def calculate_total_cost(cls, v, values):
        """Calculate total cost if not provided."""
        if v == 0.0 and 'quantity' in values and 'unit_cost' in values:
            return float(values['quantity']) * float(values['unit_cost'])
        return v


class ReorderRule(BaseModel):
    """Automatic reorder configuration."""
    rule_id: UUID = Field(default_factory=uuid4)
    product_id: str = Field(..., min_length=1)
    sku: str = Field(..., min_length=1)
    
    # Reorder parameters
    strategy: ReorderStrategy = ReorderStrategy.MIN_MAX
    reorder_point: NonNegativeInt = 0
    max_stock_level: NonNegativeInt = 0
    reorder_quantity: NonNegativeInt = 0
    economic_order_quantity: Optional[int] = None
    
    # Supplier information
    preferred_supplier_id: Optional[str] = None
    backup_supplier_ids: List[str] = Field(default_factory=list)
    lead_time_days: NonNegativeInt = 0
    safety_stock_days: NonNegativeInt = 7
    
    # Cost parameters
    carrying_cost_percentage: NonNegativeFloat = Field(default=0.25)
    ordering_cost: NonNegativeFloat = Field(default=50.0)
    stockout_cost: NonNegativeFloat = Field(default=100.0)
    
    # Seasonality and forecasting
    seasonal_adjustment: bool = False
    demand_forecast_days: NonNegativeInt = 30
    forecast_accuracy_threshold: NonNegativeFloat = Field(default=0.8, ge=0.0, le=1.0)
    
    # Configuration
    is_active: bool = True
    auto_approve_orders: bool = False
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)


class QualityCheck(BaseModel):
    """Inventory quality check record."""
    check_id: UUID = Field(default_factory=uuid4)
    batch_id: Optional[str] = None
    lot_number: Optional[str] = None
    
    # Check details
    check_type: str = Field(..., min_length=1)  # incoming, periodic, random, complaint
    check_date: datetime = Field(default_factory=datetime.utcnow)
    inspector: str = Field(..., min_length=1)
    
    # Results
    passed: bool = True
    defect_count: NonNegativeInt = 0
    sample_size: PositiveInt = 1
    defect_rate: NonNegativeFloat = Field(default=0.0, ge=0.0, le=1.0)
    
    # Issues found
    defect_types: List[str] = Field(default_factory=list)
    severity_level: str = Field(default="minor")  # minor, major, critical
    corrective_action_required: bool = False
    corrective_actions: List[str] = Field(default_factory=list)
    
    # Documentation
    photos_urls: List[str] = Field(default_factory=list)
    test_results: Dict[str, Any] = Field(default_factory=dict)
    notes: Optional[str] = None
    
    @validator('defect_rate', pre=True, always=True)
    def calculate_defect_rate(cls, v, values):
        """Calculate defect rate if not provided."""
        if v == 0.0 and 'defect_count' in values and 'sample_size' in values:
            sample_size = values['sample_size']
            if sample_size > 0:
                return float(values['defect_count']) / float(sample_size)
        return v


class InventoryForecast(BaseModel):
    """Demand forecasting for inventory planning."""
    forecast_id: UUID = Field(default_factory=uuid4)
    product_id: str = Field(..., min_length=1)
    sku: str = Field(..., min_length=1)
    
    # Forecast period
    forecast_date: datetime = Field(default_factory=datetime.utcnow)
    forecast_period_days: PositiveInt = 30
    
    # Demand predictions
    predicted_demand: NonNegativeFloat = 0.0
    confidence_interval_lower: NonNegativeFloat = 0.0
    confidence_interval_upper: NonNegativeFloat = 0.0
    confidence_level: NonNegativeFloat = Field(default=0.95, ge=0.0, le=1.0)
    
    # Model information
    model_type: str = Field(default="linear_regression")
    model_accuracy: Optional[float] = Field(None, ge=0.0, le=1.0)
    historical_data_points: NonNegativeInt = 0
    
    # Seasonal factors
    seasonal_multiplier: NonNegativeFloat = Field(default=1.0)
    trend_factor: NonNegativeFloat = Field(default=1.0)
    promotional_impact: NonNegativeFloat = Field(default=0.0)
    
    # External factors
    market_factors: Dict[str, float] = Field(default_factory=dict)
    economic_indicators: Dict[str, float] = Field(default_factory=dict)
    
    # Metadata
    created_by: str = Field(default="system")
    created_at: datetime = Field(default_factory=datetime.utcnow)


class InventoryItem(BaseModel):
    """
    Comprehensive inventory item with real-time tracking and analytics.
    
    Represents a single product's inventory across all locations with
    advanced features for demand forecasting, automatic reordering,
    and quality management.
    """
    
    # Core identifiers
    inventory_id: UUID = Field(default_factory=uuid4)
    product_id: str = Field(..., min_length=1)
    sku: str = Field(..., min_length=1, description="Stock Keeping Unit")
    
    # Product details
    product_name: str = Field(..., min_length=1)
    brand: Optional[str] = None
    category: Optional[str] = None
    subcategory: Optional[str] = None
    
    # Current inventory status
    status: InventoryStatus = InventoryStatus.IN_STOCK
    total_quantity: NonNegativeInt = 0
    available_quantity: NonNegativeInt = 0  # Total - Reserved - Damaged
    reserved_quantity: NonNegativeInt = 0
    damaged_quantity: NonNegativeInt = 0
    
    # Location breakdown
    location_quantities: Dict[str, int] = Field(default_factory=dict)
    storage_locations: List[StorageLocation] = Field(default_factory=list)
    
    # Cost and valuation
    unit_cost: NonNegativeFloat = Field(default=0.0)
    total_value: NonNegativeFloat = Field(default=0.0)
    valuation_method: InventoryValuationMethod = InventoryValuationMethod.AVERAGE_COST
    
    # Stock levels and thresholds
    minimum_stock_level: NonNegativeInt = 0
    maximum_stock_level: NonNegativeInt = 0
    reorder_point: NonNegativeInt = 0
    economic_order_quantity: Optional[int] = None
    
    # Physical attributes
    unit_of_measure: str = Field(default="each")
    weight_per_unit: Optional[float] = None
    volume_per_unit: Optional[float] = None
    dimensions: Optional[Dict[str, float]] = None  # length, width, height
    
    # Lifecycle information
    manufacturing_date: Optional[date] = None
    expiration_date: Optional[date] = None
    shelf_life_days: Optional[int] = None
    batch_id: Optional[str] = None
    lot_number: Optional[str] = None
    serial_numbers: List[str] = Field(default_factory=list)
    
    # Supplier information
    primary_supplier_id: Optional[str] = None
    supplier_sku: Optional[str] = None
    lead_time_days: NonNegativeInt = 0
    
    # Quality and compliance
    requires_quality_check: bool = False
    hazardous_material: bool = False
    temperature_controlled: bool = False
    fragile: bool = False
    
    # Analytics and performance metrics
    turnover_rate: Optional[float] = None  # Annual turnover
    carrying_cost: NonNegativeFloat = Field(default=0.0)
    obsolescence_risk: Optional[float] = Field(None, ge=0.0, le=1.0)
    abc_classification: Optional[str] = None  # A, B, C based on value
    xyz_classification: Optional[str] = None  # X, Y, Z based on demand variability
    
    # Movement history (latest movements)
    recent_movements: List[InventoryMovement] = Field(default_factory=list, max_items=50)
    last_movement_date: Optional[datetime] = None
    last_received_date: Optional[datetime] = None
    last_issued_date: Optional[datetime] = None
    
    # Reorder configuration
    reorder_rules: List[ReorderRule] = Field(default_factory=list)
    auto_reorder_enabled: bool = False
    pending_orders: List[str] = Field(default_factory=list)  # Purchase order IDs
    
    # Quality information
    quality_checks: List[QualityCheck] = Field(default_factory=list, max_items=20)
    defect_rate: NonNegativeFloat = Field(default=0.0, ge=0.0, le=1.0)
    quality_score: Optional[float] = Field(None, ge=0.0, le=100.0)
    
    # Forecasting
    demand_forecast: Optional[InventoryForecast] = None
    seasonal_pattern: Dict[int, float] = Field(default_factory=dict)  # month -> multiplier
    
    # Audit and metadata
    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)
    created_by: Optional[str] = None
    updated_by: Optional[str] = None
    
    # Data quality
    data_quality_score: float = Field(default=1.0, ge=0.0, le=1.0)
    last_physical_count_date: Optional[datetime] = None
    cycle_count_frequency_days: NonNegativeInt = 90
    
    @validator('total_value', pre=True, always=True)
    def calculate_total_value(cls, v, values):
        """Calculate total inventory value."""
        if v == 0.0 and 'total_quantity' in values and 'unit_cost' in values:
            return float(values['total_quantity']) * float(values['unit_cost'])
        return v
    
    @validator('available_quantity', pre=True, always=True)
    def calculate_available_quantity(cls, v, values):
        """Calculate available quantity."""
        if 'total_quantity' in values:
            total = values['total_quantity']
            reserved = values.get('reserved_quantity', 0)
            damaged = values.get('damaged_quantity', 0)
            return total - reserved - damaged
        return v
    
    @model_validator(mode="before")
    @classmethod
    def validate_stock_levels(cls, values):
        """Validate stock level consistency."""
        total_qty = values.get('total_quantity', 0)
        reserved_qty = values.get('reserved_quantity', 0)
        damaged_qty = values.get('damaged_quantity', 0)
        
        if reserved_qty + damaged_qty > total_qty:
            raise ValueError("Reserved + damaged quantity cannot exceed total quantity")
        
        # Validate reorder point makes sense
        min_stock = values.get('minimum_stock_level', 0)
        reorder_point = values.get('reorder_point', 0)
        
        if reorder_point > 0 and reorder_point < min_stock:
            values['reorder_point'] = min_stock
        
        return values
    
    def is_low_stock(self) -> bool:
        """Check if inventory is below reorder point."""
        if self.reorder_point > 0:
            return self.available_quantity <= self.reorder_point
        return self.available_quantity <= self.minimum_stock_level
    
    def is_out_of_stock(self) -> bool:
        """Check if inventory is completely out of stock."""
        return self.available_quantity <= 0
    
    def days_until_expiry(self) -> Optional[int]:
        """Calculate days until expiration."""
        if not self.expiration_date:
            return None
        
        today = date.today()
        if self.expiration_date <= today:
            return 0
        
        return (self.expiration_date - today).days
    
    def is_expired(self) -> bool:
        """Check if inventory is expired."""
        if not self.expiration_date:
            return False
        return self.expiration_date < date.today()
    
    def is_near_expiry(self, days_threshold: int = 30) -> bool:
        """Check if inventory is nearing expiration."""
        days_left = self.days_until_expiry()
        if days_left is None:
            return False
        return 0 < days_left <= days_threshold
    
    def calculate_carrying_cost(self, annual_rate: float = 0.25) -> float:
        """Calculate annual carrying cost."""
        return float(self.total_value) * annual_rate
    
    def update_inventory_status(self):
        """Update inventory status based on current conditions."""
        if self.is_expired():
            self.status = InventoryStatus.EXPIRED
        elif self.is_out_of_stock():
            self.status = InventoryStatus.OUT_OF_STOCK
        elif self.is_low_stock():
            self.status = InventoryStatus.LOW_STOCK
        elif self.pending_orders:
            self.status = InventoryStatus.ON_ORDER
        else:
            self.status = InventoryStatus.IN_STOCK
    
    def add_movement(self, movement: InventoryMovement):
        """Add inventory movement and update quantities."""
        # Add to recent movements
        self.recent_movements.insert(0, movement)
        if len(self.recent_movements) > 50:
            self.recent_movements.pop()
        
        # Update quantities based on movement type
        if movement.movement_type == MovementType.INBOUND:
            self.total_quantity += movement.quantity
            self.last_received_date = movement.created_at
        elif movement.movement_type == MovementType.OUTBOUND:
            self.total_quantity -= movement.quantity
            self.last_issued_date = movement.created_at
        elif movement.movement_type == MovementType.ADJUSTMENT:
            # Adjustment can be positive or negative
            self.total_quantity += movement.quantity
        elif movement.movement_type == MovementType.DAMAGE:
            self.damaged_quantity += abs(movement.quantity)
        elif movement.movement_type == MovementType.RETURN:
            self.total_quantity += movement.quantity
        
        # Update last movement date
        self.last_movement_date = movement.created_at
        self.updated_at = datetime.utcnow()
        
        # Recalculate derived values
        self.available_quantity = self.total_quantity - self.reserved_quantity - self.damaged_quantity
        self.total_value = float(self.total_quantity) * float(self.unit_cost)
        
        # Update status
        self.update_inventory_status()
    
    def reserve_quantity(self, quantity: int, reference_id: Optional[str] = None) -> bool:
        """Reserve inventory for orders."""
        if self.available_quantity >= quantity:
            self.reserved_quantity += quantity
            self.available_quantity -= quantity
            
            # Create movement record
            movement = InventoryMovement(
                product_id=self.product_id,
                sku=self.sku,
                movement_type=MovementType.OUTBOUND,
                quantity=-quantity,  # Negative for reservation
                reference_type="reservation",
                reference_id=reference_id,
                created_by="system"
            )
            self.add_movement(movement)
            
            return True
        return False
    
    def release_reservation(self, quantity: int, reference_id: Optional[str] = None):
        """Release reserved inventory."""
        release_qty = min(quantity, self.reserved_quantity)
        if release_qty > 0:
            self.reserved_quantity -= release_qty
            self.available_quantity += release_qty
            
            # Create movement record
            movement = InventoryMovement(
                product_id=self.product_id,
                sku=self.sku,
                movement_type=MovementType.ADJUSTMENT,
                quantity=release_qty,
                reference_type="reservation_release",
                reference_id=reference_id,
                created_by="system"
            )
            self.add_movement(movement)
    
    def calculate_abc_classification(self, annual_usage_value: float, 
                                   total_annual_value: float) -> str:
        """Calculate ABC classification based on annual usage value."""
        if total_annual_value == 0:
            return "C"
        
        percentage = (annual_usage_value / total_annual_value) * 100
        
        if percentage >= 20:  # Top 20% of value
            self.abc_classification = "A"
        elif percentage >= 5:   # Next 30% of value (5-20%)
            self.abc_classification = "B"
        else:                   # Remaining 50% of value
            self.abc_classification = "C"
        
        return self.abc_classification
    
    def calculate_xyz_classification(self, demand_coefficient_of_variation: float) -> str:
        """Calculate XYZ classification based on demand variability."""
        if demand_coefficient_of_variation <= 0.5:
            self.xyz_classification = "X"  # Low variability
        elif demand_coefficient_of_variation <= 1.0:
            self.xyz_classification = "Y"  # Medium variability
        else:
            self.xyz_classification = "Z"  # High variability
        
        return self.xyz_classification
    
    def needs_reorder(self) -> bool:
        """Check if item needs to be reordered."""
        if not self.auto_reorder_enabled:
            return False
        
        # Check if already have pending orders
        if self.pending_orders:
            return False
        
        # Check reorder point
        return self.is_low_stock()
    
    def calculate_suggested_order_quantity(self, demand_forecast_days: int = 30) -> int:
        """Calculate suggested order quantity based on EOQ and forecast."""
        if self.economic_order_quantity:
            return self.economic_order_quantity
        
        # Simple calculation based on lead time and forecast
        if self.demand_forecast and self.lead_time_days > 0:
            daily_demand = self.demand_forecast.predicted_demand / self.demand_forecast.forecast_period_days
            lead_time_demand = daily_demand * self.lead_time_days
            safety_stock = lead_time_demand * 0.2  # 20% safety stock
            
            # Order enough to reach max stock level
            target_quantity = int(self.maximum_stock_level or (lead_time_demand + safety_stock))
            order_quantity = max(0, target_quantity - self.total_quantity)
            
            return order_quantity
        
        # Fallback to difference between max and current stock
        if self.maximum_stock_level > 0:
            return max(0, self.maximum_stock_level - self.total_quantity)
        
        return self.minimum_stock_level or 100  # Default order quantity
    
    def to_analytics_dict(self) -> Dict[str, Any]:
        """Convert to dictionary optimized for analytics."""
        return {
            'inventory_id': str(self.inventory_id),
            'product_id': self.product_id,
            'sku': self.sku,
            'product_name': self.product_name,
            'brand': self.brand,
            'category': self.category,
            'subcategory': self.subcategory,
            
            # Quantities
            'total_quantity': self.total_quantity,
            'available_quantity': self.available_quantity,
            'reserved_quantity': self.reserved_quantity,
            'damaged_quantity': self.damaged_quantity,
            
            # Financial
            'unit_cost': float(self.unit_cost),
            'total_value': float(self.total_value),
            'carrying_cost': float(self.carrying_cost),
            
            # Status and classifications
            'status': self.status.value,
            'abc_classification': self.abc_classification,
            'xyz_classification': self.xyz_classification,
            'turnover_rate': self.turnover_rate,
            
            # Stock levels
            'minimum_stock_level': self.minimum_stock_level,
            'maximum_stock_level': self.maximum_stock_level,
            'reorder_point': self.reorder_point,
            
            # Dates and lifecycle
            'days_until_expiry': self.days_until_expiry(),
            'is_expired': self.is_expired(),
            'is_near_expiry': self.is_near_expiry(),
            'manufacturing_date': self.manufacturing_date.isoformat() if self.manufacturing_date else None,
            'expiration_date': self.expiration_date.isoformat() if self.expiration_date else None,
            
            # Movement metrics
            'last_movement_date': self.last_movement_date.isoformat() if self.last_movement_date else None,
            'last_received_date': self.last_received_date.isoformat() if self.last_received_date else None,
            'last_issued_date': self.last_issued_date.isoformat() if self.last_issued_date else None,
            
            # Quality
            'defect_rate': float(self.defect_rate),
            'quality_score': self.quality_score,
            'requires_quality_check': self.requires_quality_check,
            
            # Forecasting
            'predicted_demand': self.demand_forecast.predicted_demand if self.demand_forecast else None,
            'forecast_confidence': self.demand_forecast.confidence_level if self.demand_forecast else None,
            
            # Flags
            'is_low_stock': self.is_low_stock(),
            'is_out_of_stock': self.is_out_of_stock(),
            'needs_reorder': self.needs_reorder(),
            'auto_reorder_enabled': self.auto_reorder_enabled,
            'has_pending_orders': bool(self.pending_orders),
            'hazardous_material': self.hazardous_material,
            'temperature_controlled': self.temperature_controlled,
            
            # Supplier
            'primary_supplier_id': self.primary_supplier_id,
            'lead_time_days': self.lead_time_days,
            
            # Location count
            'location_count': len(self.location_quantities),
            'storage_locations_count': len(self.storage_locations),
            
            # Data quality
            'data_quality_score': self.data_quality_score,
            'last_physical_count_date': self.last_physical_count_date.isoformat() if self.last_physical_count_date else None,
            
            # Audit
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat()
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


class InventoryAggregate(BaseModel):
    """
    Inventory aggregate for multi-location and multi-product analytics.
    """
    
    # Aggregation identifiers
    aggregate_id: UUID = Field(default_factory=uuid4)
    aggregation_type: str = Field(..., description="product, category, location, supplier, etc.")
    aggregation_key: str = Field(..., description="The key being aggregated by")
    
    # Time period
    period_start: datetime
    period_end: datetime
    period_type: str = Field(default="daily")  # daily, weekly, monthly, quarterly, yearly
    
    # Quantity metrics
    total_quantity: NonNegativeInt = 0
    total_value: NonNegativeFloat = 0.0
    average_unit_cost: NonNegativeFloat = 0.0
    
    # Movement metrics
    inbound_quantity: NonNegativeInt = 0
    outbound_quantity: NonNegativeInt = 0
    adjustment_quantity: int = 0
    total_movements: NonNegativeInt = 0
    
    # Financial metrics
    carrying_cost_total: NonNegativeFloat = 0.0
    obsolescence_cost: NonNegativeFloat = 0.0
    stockout_cost: NonNegativeFloat = 0.0
    
    # Performance metrics
    inventory_turnover: Optional[float] = None
    days_sales_outstanding: Optional[float] = None
    fill_rate: Optional[float] = Field(None, ge=0.0, le=1.0)
    service_level: Optional[float] = Field(None, ge=0.0, le=1.0)
    
    # Quality metrics
    average_defect_rate: NonNegativeFloat = Field(default=0.0, ge=0.0, le=1.0)
    quality_incidents: NonNegativeInt = 0
    
    # Items breakdown
    total_items: NonNegativeInt = 0
    low_stock_items: NonNegativeInt = 0
    out_of_stock_items: NonNegativeInt = 0
    expired_items: NonNegativeInt = 0
    
    # Location information (if applicable)
    locations_count: NonNegativeInt = 0
    primary_locations: List[str] = Field(default_factory=list)
    
    # Metadata
    calculated_at: datetime = Field(default_factory=datetime.utcnow)
    data_source: str = Field(default="system")
    
    def calculate_inventory_turnover(self, cost_of_goods_sold: float) -> float:
        """Calculate inventory turnover ratio."""
        if self.total_value > 0:
            self.inventory_turnover = cost_of_goods_sold / float(self.total_value)
        else:
            self.inventory_turnover = 0.0
        return self.inventory_turnover
    
    def calculate_days_sales_outstanding(self) -> Optional[float]:
        """Calculate average days of inventory on hand."""
        if self.inventory_turnover and self.inventory_turnover > 0:
            self.days_sales_outstanding = 365.0 / self.inventory_turnover
        return self.days_sales_outstanding


# Factory functions

def create_inventory_item(
    product_id: str,
    sku: str,
    product_name: str,
    initial_quantity: int = 0,
    unit_cost: float = 0.0,
    **kwargs
) -> InventoryItem:
    """Factory function to create an inventory item."""
    
    item_data = {
        'product_id': product_id,
        'sku': sku,
        'product_name': product_name,
        'total_quantity': initial_quantity,
        'unit_cost': unit_cost,
        **kwargs
    }
    
    item = InventoryItem(**item_data)
    item.update_inventory_status()
    
    return item


def create_sample_inventory() -> List[InventoryItem]:
    """Create sample inventory items for testing."""
    
    items = [
        create_inventory_item(
            product_id="PROD-001",
            sku="SKU-001",
            product_name="Wireless Bluetooth Headphones",
            initial_quantity=150,
            unit_cost=75.00,
            brand="AudioTech",
            category="Electronics",
            subcategory="Audio",
            minimum_stock_level=25,
            maximum_stock_level=200,
            reorder_point=50,
            auto_reorder_enabled=True
        ),
        create_inventory_item(
            product_id="PROD-002", 
            sku="SKU-002",
            product_name="Organic Cotton T-Shirt",
            initial_quantity=300,
            unit_cost=15.50,
            brand="EcoWear",
            category="Clothing",
            subcategory="Tops",
            minimum_stock_level=50,
            maximum_stock_level=500,
            reorder_point=75
        ),
        create_inventory_item(
            product_id="PROD-003",
            sku="SKU-003", 
            product_name="Stainless Steel Water Bottle",
            initial_quantity=85,
            unit_cost=22.99,
            brand="HydroLife",
            category="Home & Garden",
            subcategory="Drinkware",
            minimum_stock_level=20,
            maximum_stock_level=150,
            reorder_point=30,
            auto_reorder_enabled=True
        )
    ]
    
    return items