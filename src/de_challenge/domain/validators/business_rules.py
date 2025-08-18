"""Business rules validation for domain entities."""

import re
from collections import defaultdict
from datetime import datetime
from decimal import Decimal
from typing import Any

from ...core.constants import (
    CANCELLED_INVOICE_PREFIX,
    MAX_VALID_PRICE,
    MAX_VALID_QUANTITY,
    MIN_VALID_PRICE,
    MIN_VALID_QUANTITY,
    STOCK_CODE_PATTERN,
)
from ...core.exceptions import ValidationException
from ...core.logging import get_logger
from ..entities.customer import Customer, CustomerMetrics
from ..entities.invoice import Invoice
from ..entities.product import Product
from ..entities.transaction import Transaction, TransactionLine

logger = get_logger(__name__)


class BusinessRuleValidator:
    """
    Validates domain entities against business rules.
    Implements comprehensive validation logic for retail data.
    """

    def __init__(self, strict_mode: bool = False) -> None:
        """
        Initialize validator.

        Args:
            strict_mode: If True, fail on any validation error.
                        If False, mark invalid but continue.
        """
        self.strict_mode = strict_mode
        self.validation_stats: dict[str, int] = defaultdict(int)

    def validate_transaction_line(
        self,
        line: TransactionLine,
        context: dict[str, Any] | None = None
    ) -> tuple[bool, list[str]]:
        """
        Validate a transaction line against business rules.

        Args:
            line: Transaction line to validate
            context: Optional context for validation

        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []
        context = context or {}

        # Rule 1: Invoice number format
        if not line.invoice_no:
            errors.append("Missing invoice number")
        elif len(line.invoice_no) > 20:
            errors.append(f"Invoice number too long: {line.invoice_no}")

        # Rule 2: Stock code validation
        if not self._validate_stock_code(line.stock_code):
            errors.append(f"Invalid stock code: {line.stock_code}")

        # Rule 3: Quantity validation
        if not self._validate_quantity(line.quantity, line.is_cancelled):
            errors.append(
                f"Invalid quantity {line.quantity} for "
                f"{'cancelled' if line.is_cancelled else 'normal'} transaction"
            )

        # Rule 4: Price validation
        if not self._validate_price(line.unit_price, line.is_return):
            errors.append(
                f"Invalid price {line.unit_price} for {'return' if line.is_return else 'sale'}")

        # Rule 5: Date validation
        if not self._validate_date(line.invoice_date):
            errors.append(f"Invalid date: {line.invoice_date}")

        # Rule 6: Customer ID required for non-cancelled
        if not line.is_cancelled and not line.customer_id:
            errors.append(
                "Customer ID required for non-cancelled transactions")

        # Rule 7: Description required for regular products
        special_codes = ["POST", "DOT", "M",
                         "BANK CHARGES", "PADS", "AMAZONFEE"]
        if line.stock_code not in special_codes and not line.description:
            errors.append(f"Missing description for product {line.stock_code}")

        # Rule 8: Country validation
        if line.country and not self._validate_country(line.country):
            errors.append(f"Invalid country: {line.country}")

        # Update stats
        self.validation_stats["transaction_lines_validated"] += 1
        if errors:
            self.validation_stats["transaction_lines_failed"] += 1

        return (len(errors) == 0, errors)

    def validate_transaction(
        self,
        transaction: Transaction,
        context: dict[str, Any] | None = None
    ) -> tuple[bool, list[str]]:
        """
        Validate a complete transaction.

        Args:
            transaction: Transaction to validate
            context: Optional context

        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []

        # Validate transaction level rules
        if not transaction.lines:
            errors.append("Transaction has no line items")

        # Validate each line
        for i, line in enumerate(transaction.lines):
            line_valid, line_errors = self.validate_transaction_line(
                line, context)
            if line_errors:
                errors.extend([f"Line {i+1}: {e}" for e in line_errors])

        # Cross-line validation
        if transaction.lines:
            # All lines should have same customer
            customer_ids = {
                line.customer_id for line in transaction.lines
                if line.customer_id
            }
            if len(customer_ids) > 1:
                errors.append(f"Multiple customer IDs: {customer_ids}")

            # All lines should have same date
            dates = {line.invoice_date.date() for line in transaction.lines}
            if len(dates) > 1:
                errors.append(f"Multiple dates in transaction: {dates}")

            # All lines should have same country
            countries = {
                line.country for line in transaction.lines
                if line.country
            }
            if len(countries) > 1:
                errors.append(f"Multiple countries: {countries}")

        # Business logic validation
        if transaction.total_amount and transaction.total_amount < 0:
            if not transaction.invoice_no.startswith(CANCELLED_INVOICE_PREFIX):
                errors.append("Negative total for non-cancelled transaction")

        self.validation_stats["transactions_validated"] += 1
        if errors:
            self.validation_stats["transactions_failed"] += 1

        return (len(errors) == 0, errors)

    def validate_product(
        self,
        product: Product,
        context: dict[str, Any] | None = None
    ) -> tuple[bool, list[str]]:
        """
        Validate a product entity.

        Args:
            product: Product to validate
            context: Optional context

        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []

        # Rule 1: Stock code format
        if not self._validate_stock_code(product.stock_code):
            errors.append(f"Invalid stock code: {product.stock_code}")

        # Rule 2: Description required
        if not product.description or len(product.description) < 3:
            errors.append("Product description too short")

        # Rule 3: Price validation
        if product.base_price <= 0:
            errors.append(f"Invalid base price: {product.base_price}")

        # Rule 4: Price consistency
        if product.min_price and product.max_price:
            if product.min_price > product.max_price:
                errors.append(
                    f"Min price ({product.min_price}) > Max price ({product.max_price})"
                )
            if product.base_price < product.min_price:
                errors.append("Base price below minimum")
            if product.base_price > product.max_price:
                errors.append("Base price above maximum")

        # Rule 5: Return rate threshold
        if product.return_rate and product.return_rate > 50:
            errors.append(f"High return rate: {product.return_rate:.2f}%")

        # Rule 6: Metrics consistency
        if product.total_returned and product.total_sold:
            if product.total_returned > product.total_sold:
                errors.append("Returns exceed sales")

        self.validation_stats["products_validated"] += 1
        if errors:
            self.validation_stats["products_failed"] += 1

        return (len(errors) == 0, errors)

    def validate_customer(
        self,
        customer: Customer,
        context: dict[str, Any] | None = None
    ) -> tuple[bool, list[str]]:
        """
        Validate a customer entity.

        Args:
            customer: Customer to validate
            context: Optional context

        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []

        # Rule 1: Customer ID format (should be numeric in this dataset)
        try:
            float(customer.customer_id)
        except ValueError:
            errors.append(
                f"Invalid customer ID format: {customer.customer_id}")

        # Rule 2: Country required
        if not customer.country:
            errors.append("Country is required")

        # Rule 3: Email format if provided
        if customer.email:
            # Pydantic EmailStr handles this validation
            pass

        # Rule 4: Segment consistency
        if customer.is_vip and customer.segment.value not in ["vip", "frequent"]:
            errors.append(
                f"VIP flag inconsistent with segment: {customer.segment}")

        # Rule 5: Activity consistency
        if not customer.is_active and customer.segment.value in ["vip", "frequent"]:
            errors.append("Inactive customer with active segment")

        # Rule 6: Metrics validation
        if customer.metrics:
            metrics_valid, metrics_errors = self._validate_customer_metrics(
                customer.metrics
            )
            if metrics_errors:
                errors.extend([f"Metrics: {e}" for e in metrics_errors])

        self.validation_stats["customers_validated"] += 1
        if errors:
            self.validation_stats["customers_failed"] += 1

        return (len(errors) == 0, errors)

    def validate_invoice(
        self,
        invoice: Invoice,
        context: dict[str, Any] | None = None
    ) -> tuple[bool, list[str]]:
        """
        Validate an invoice entity.

        Args:
            invoice: Invoice to validate
            context: Optional context

        Returns:
            Tuple of (is_valid, list_of_errors)
        """
        errors = []

        # Rule 1: Invoice number format
        if not invoice.invoice_no:
            errors.append("Missing invoice number")

        # Rule 2: Date validation
        if not self._validate_date(invoice.invoice_date):
            errors.append(f"Invalid invoice date: {invoice.invoice_date}")

        # Rule 3: Cancelled invoice checks
        if invoice.is_cancelled:
            if not invoice.invoice_no.startswith(CANCELLED_INVOICE_PREFIX):
                errors.append("Cancelled flag but invoice number not prefixed")

        # Rule 4: Amount validation
        if invoice.total_amount < 0 and invoice.invoice_type.value == "sale":
            errors.append("Sale invoice cannot have negative total")

        # Rule 5: Customer required for completed
        if invoice.status.value == "completed" and not invoice.customer_id:
            errors.append("Completed invoice requires customer ID")

        # Rule 6: Items required
        if invoice.total_items == 0 and invoice.status.value == "completed":
            errors.append("Completed invoice must have items")

        # Rule 7: Tax validation
        if invoice.tax_amount and invoice.tax_amount > invoice.subtotal:
            errors.append("Tax amount exceeds subtotal")

        self.validation_stats["invoices_validated"] += 1
        if errors:
            self.validation_stats["invoices_failed"] += 1

        return (len(errors) == 0, errors)

    def _validate_stock_code(self, stock_code: str) -> bool:
        """Validate stock code format."""
        if not stock_code:
            return False

        # Special valid codes
        special_codes = ["POST", "DOT", "M",
                         "BANK CHARGES", "PADS", "AMAZONFEE"]
        if stock_code in special_codes:
            return True

        # Check pattern
        return bool(re.match(STOCK_CODE_PATTERN, stock_code))

    def _validate_quantity(self, quantity: int, is_cancelled: bool) -> bool:
        """Validate quantity based on transaction type."""
        if quantity < MIN_VALID_QUANTITY or quantity > MAX_VALID_QUANTITY:
            return False

        # Cancelled transactions typically have negative quantities
        if is_cancelled and quantity > 0:
            return False  # Warning, not always error

        return True

    def _validate_price(self, price: Decimal, is_return: bool) -> bool:
        """Validate price based on transaction type."""
        if price < MIN_VALID_PRICE or price > MAX_VALID_PRICE:
            return False

        # Returns can have zero price
        if is_return and price == 0:
            return True

        # Normal sales should have positive price
        if not is_return and price <= 0:
            return False

        return True

    def _validate_date(self, date: datetime) -> bool:
        """Validate date is reasonable."""
        # Not in future
        if date > datetime.utcnow():
            return False

        # Not too old (e.g., before year 2000)
        if date.year < 2000:
            return False

        return True

    def _validate_country(self, country: str) -> bool:
        """Validate country name."""
        if not country or len(country) < 2:
            return False

        # Could add more sophisticated country validation
        return True

    def _validate_customer_metrics(
        self,
        metrics: CustomerMetrics
    ) -> tuple[bool, list[str]]:
        """Validate customer metrics."""
        errors = []

        # Date consistency
        if metrics.first_purchase_date and metrics.last_purchase_date:
            if metrics.first_purchase_date > metrics.last_purchase_date:
                errors.append("First purchase after last purchase")

        # Transaction consistency
        if metrics.total_transactions == 0 and metrics.total_spent > 0:
            errors.append("Spending without transactions")

        # Return rate
        if metrics.return_rate and metrics.return_rate > 100:
            errors.append(f"Invalid return rate: {metrics.return_rate}%")

        return (len(errors) == 0, errors)

    def get_validation_report(self) -> dict[str, Any]:
        """Get validation statistics report."""
        total_validated = sum(
            v for k, v in self.validation_stats.items()
            if k.endswith("_validated")
        )
        total_failed = sum(
            v for k, v in self.validation_stats.items()
            if k.endswith("_failed")
        )

        success_rate = (
            ((total_validated - total_failed) / total_validated * 100)
            if total_validated > 0 else 0
        )

        return {
            "total_validated": total_validated,
            "total_failed": total_failed,
            "success_rate": f"{success_rate:.2f}%",
            "details": dict(self.validation_stats),
        }

    def reset_stats(self) -> None:
        """Reset validation statistics."""
        self.validation_stats.clear()


# Convenience functions
def validate_transaction(
    transaction: Transaction | TransactionLine,
    strict: bool = False
) -> tuple[bool, list[str]]:
    """Validate a transaction or transaction line."""
    validator = BusinessRuleValidator(strict_mode=strict)

    if isinstance(transaction, Transaction):
        return validator.validate_transaction(transaction)
    else:
        return validator.validate_transaction_line(transaction)


def validate_product(product: Product, strict: bool = False) -> tuple[bool, list[str]]:
    """Validate a product."""
    validator = BusinessRuleValidator(strict_mode=strict)
    return validator.validate_product(product)


def validate_customer(customer: Customer, strict: bool = False) -> tuple[bool, list[str]]:
    """Validate a customer."""
    validator = BusinessRuleValidator(strict_mode=strict)
    return validator.validate_customer(customer)


def validate_invoice(invoice: Invoice, strict: bool = False) -> tuple[bool, list[str]]:
    """Validate an invoice."""
    validator = BusinessRuleValidator(strict_mode=strict)
    return validator.validate_invoice(invoice)


def bulk_validate(
    entities: list[Any],
    entity_type: str,
    strict: bool = False,
    batch_size: int = 1000
) -> dict[str, Any]:
    """
    Validate multiple entities in batches.

    Args:
        entities: List of entities to validate
        entity_type: Type of entity (transaction, product, customer, invoice)
        strict: Strict validation mode
        batch_size: Size of validation batches

    Returns:
        Validation report
    """
    validator = BusinessRuleValidator(strict_mode=strict)

    validation_methods = {
        "transaction": validator.validate_transaction,
        "transaction_line": validator.validate_transaction_line,
        "product": validator.validate_product,
        "customer": validator.validate_customer,
        "invoice": validator.validate_invoice,
    }

    if entity_type not in validation_methods:
        raise ValueError(f"Unknown entity type: {entity_type}")

    validate_func = validation_methods[entity_type]

    failed_entities = []
    for i in range(0, len(entities), batch_size):
        batch = entities[i:i + batch_size]

        for entity in batch:
            is_valid, errors = validate_func(entity)
            if not is_valid:
                failed_entities.append({
                    "entity": entity,
                    "errors": errors,
                })

                if strict:
                    raise ValidationException(
                        f"Validation failed for {entity_type}",
                        details={"errors": errors}
                    )

    report = validator.get_validation_report()
    report["failed_entities"] = failed_entities[:100]  # Limit to first 100

    return report
