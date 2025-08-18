"""Domain layer containing business entities and validation logic."""

from .entities.customer import (
    Customer,
    CustomerMetrics,
    CustomerSegment,
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

__all__ = [
    # Transaction entities
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
    "CustomerMetrics",
    # Invoice entities
    "Invoice",
    "InvoiceStatus",
    "InvoiceType",
    # Validators
    "BusinessRuleValidator",
    "validate_transaction",
    "validate_product",
    "validate_customer",
    "DataQualityValidator",
    "QualityMetrics",
    "ValidationResult",
]
