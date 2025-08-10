"""Domain layer containing business entities and validation logic."""

from .entities.transaction import (
    Transaction,
    TransactionLine,
    CancelledTransaction,
    TransactionStatus,
)
from .entities.product import (
    Product,
    ProductCategory,
    StockItem,
)
from .entities.customer import (
    Customer,
    CustomerSegment,
    CustomerMetrics,
)
from .entities.invoice import (
    Invoice,
    InvoiceStatus,
    InvoiceType,
)
from .validators.business_rules import (
    BusinessRuleValidator,
    validate_transaction,
    validate_product,
    validate_customer,
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
