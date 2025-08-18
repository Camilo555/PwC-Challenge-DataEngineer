"""Domain interfaces for dependency inversion."""

from .repository_interface import (
    ICustomerRepository,
    IInvoiceRepository,
    IProductRepository,
    IRepository,
    ITransactionRepository,
)
from .service_interface import (
    ISearchService,
    IService,
    ITransformationService,
    IValidationService,
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
