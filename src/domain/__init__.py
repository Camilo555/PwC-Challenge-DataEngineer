"""Domain layer containing business entities and validation logic."""

# Core entities
from .entities.customer import (
    CustomerEntity as Customer,
)
from .entities.customer import (
    CustomerSegment,
    CustomerStatus,
    CustomerValueTier,
)
from .entities.invoice import (
    Invoice,
    InvoiceStatus,
    InvoiceType,
)
from .entities.product import (
    Product,
    ProductCategory,
    StockItem,
)
from .entities.transaction import (
    CancelledTransaction,
    Transaction,
    TransactionLine,
    TransactionStatus,
)

# Advanced domain entities
from .entities.analytics import (
    AnalyticsMetric,
    MetricType,
    TimeGranularity,
)
from .entities.warehouse import (
    ETLJob,
    DataLayerType,
    DataQualityStatus,
    ProcessingStatus,
)
from .entities.fulfillment import (
    FulfillmentStatus,
    ShippingMethod,
)
from .entities.inventory import (
    InventoryItem,
    InventoryStatus,
    StockMovement,
)
from .entities.order import (
    OrderEntity,
    OrderStatus,
    OrderPriority,
)

# Domain aggregates
from .customers import CustomerDomain
from .sales import SalesDomain

# Services
from .services.sales_service import SalesService

# Validators
from .validators.business_rules import (
    BusinessRuleValidator,
    validate_customer,
    validate_product,
    validate_transaction,
)
from .validators.data_quality import (
    DataQualityValidator,
    QualityMetrics,
    ValidationResult,
)
from .validators.advanced_data_quality import (
    AdvancedDataQualityValidator,
    DataQualityMetrics,
)

__all__ = [
    # Core transaction entities
    "Transaction",
    "TransactionLine",
    "CancelledTransaction",
    "TransactionStatus",

    # Product entities
    "Product",
    "ProductCategory",
    "StockItem",

    # Customer entities
    "Customer",
    "CustomerSegment",
    "CustomerValueTier",
    "CustomerStatus",

    # Invoice entities
    "Invoice",
    "InvoiceStatus",
    "InvoiceType",

    # Advanced domain entities
    "AnalyticsMetric",
    "MetricType",
    "TimeGranularity",
    "ETLJob",
    "DataLayerType",
    "DataQualityStatus",
    "ProcessingStatus",
    "FulfillmentStatus",
    "ShippingMethod",
    "InventoryItem",
    "InventoryStatus",
    "StockMovement",
    "OrderEntity",
    "OrderStatus",
    "OrderPriority",

    # Domain aggregates
    "CustomerDomain",
    "SalesDomain",

    # Services
    "SalesService",

    # Validators
    "BusinessRuleValidator",
    "validate_transaction",
    "validate_product",
    "validate_customer",
    "DataQualityValidator",
    "QualityMetrics",
    "ValidationResult",
    "AdvancedDataQualityValidator",
    "DataQualityMetrics",
]
