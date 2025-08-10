"""Domain interfaces for dependency inversion."""

from .repository_interface import (
    IRepository,
    ITransactionRepository,
    IProductRepository,
    ICustomerRepository,
    IInvoiceRepository,
)
from .service_interface import (
    IService,
    IValidationService,
    ITransformationService,
    ISearchService,
)

__all__ = [
    # Repository interfaces
    "IRepository",
    "ITransactionRepository",
    "IProductRepository",
    "ICustomerRepository",
    "IInvoiceRepository",
    # Service interfaces
    "IService",
    "IValidationService",
    "ITransformationService",
    "ISearchService",
]
