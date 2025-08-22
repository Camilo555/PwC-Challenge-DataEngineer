"""
Business Rule Validators
Centralized business logic validation for domain entities.
"""
from datetime import datetime, date
from decimal import Decimal
from typing import Any, List, Optional


class BusinessRuleValidator:
    """
    Centralized business rule validation for the retail domain.
    """
    
    # Business constants
    MAX_TRANSACTION_AMOUNT = Decimal('100000')  # £100k limit
    MIN_TRANSACTION_AMOUNT = Decimal('0.01')    # £0.01 minimum
    MAX_QUANTITY_PER_ITEM = 10000               # Maximum quantity per line item
    
    @classmethod
    def validate_transaction_amount(cls, amount: Decimal, context: str = "transaction") -> Decimal:
        """
        Validate transaction amount against business rules.
        """
        if amount <= 0:
            raise ValueError(f"{context} amount must be positive")
        
        if amount < cls.MIN_TRANSACTION_AMOUNT:
            raise ValueError(
                f"{context} amount (£{amount}) is below minimum allowed (£{cls.MIN_TRANSACTION_AMOUNT})"
            )
        
        if amount > cls.MAX_TRANSACTION_AMOUNT:
            raise ValueError(
                f"{context} amount (£{amount}) exceeds maximum allowed (£{cls.MAX_TRANSACTION_AMOUNT})"
            )
        
        return amount
    
    @classmethod
    def validate_customer_segment(cls, lifetime_value: Decimal, order_count: int) -> str:
        """
        Validate and determine customer segment based on business rules.
        """
        cls.validate_transaction_amount(lifetime_value, "lifetime value")
        
        if order_count < 0:
            raise ValueError("Order count cannot be negative")
        
        # Apply business rules for segmentation
        if lifetime_value >= Decimal('1000') and order_count >= 10:
            return "VIP"
        elif lifetime_value >= Decimal('500') and order_count >= 5:
            return "Premium"
        elif lifetime_value >= Decimal('100') and order_count >= 2:
            return "Regular"
        else:
            return "New"
    
    @classmethod
    def validate_date_range(cls, start_date: date, end_date: date, context: str = "date range") -> tuple[date, date]:
        """
        Validate date range against business rules.
        """
        if start_date > end_date:
            raise ValueError(f"{context}: start date cannot be after end date")
        
        # Business rule: Maximum date range of 2 years for operational queries
        if (end_date - start_date).days > 730:
            raise ValueError(f"{context}: date range cannot exceed 730 days for performance reasons")
        
        return start_date, end_date


# Individual validation functions
def validate_transaction(transaction: Any) -> bool:
    """Validate a transaction entity."""
    try:
        if hasattr(transaction, 'total_amount'):
            BusinessRuleValidator.validate_transaction_amount(transaction.total_amount)
        return True
    except ValueError:
        return False


def validate_customer(customer: Any) -> bool:
    """Validate a customer entity."""
    try:
        if hasattr(customer, 'lifetime_value') and hasattr(customer, 'order_count'):
            BusinessRuleValidator.validate_customer_segment(
                customer.lifetime_value, customer.order_count
            )
        return True
    except ValueError:
        return False


def validate_product(product: Any) -> bool:
    """Validate a product entity."""
    try:
        if hasattr(product, 'unit_price') and product.unit_price is not None:
            BusinessRuleValidator.validate_transaction_amount(product.unit_price, "product price")
        return True
    except ValueError:
        return False


def validate_invoice(invoice: Any) -> bool:
    """Validate an invoice entity."""
    try:
        if hasattr(invoice, 'total_amount'):
            BusinessRuleValidator.validate_transaction_amount(invoice.total_amount, "invoice")
        return True
    except ValueError:
        return False


def bulk_validate(entities: List[Any], validator_func: callable) -> List[bool]:
    """
    Bulk validate a list of entities using the specified validator function.
    
    Args:
        entities: List of entities to validate
        validator_func: Function to use for validation
        
    Returns:
        List of boolean results indicating validation success for each entity
    """
    return [validator_func(entity) for entity in entities]