"""
Domain Entities for Sales - Separate from Persistence
Pure business logic without ORM dependencies.
Implements DDD principles with rich domain models and business rules.
"""

from dataclasses import dataclass, field
from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional, Dict, Any
from enum import Enum
from uuid import uuid4


class InvoiceStatus(Enum):
    """Invoice status enumeration following business rules."""
    DRAFT = 'draft'
    PENDING = 'pending'
    PAID = 'paid'
    CANCELLED = 'cancelled'
    REFUNDED = 'refunded'


class PaymentMethod(Enum):
    """Payment method enumeration."""
    CASH = 'cash'
    CREDIT_CARD = 'credit_card'
    DEBIT_CARD = 'debit_card'
    BANK_TRANSFER = 'bank_transfer'
    DIGITAL_WALLET = 'digital_wallet'
    CHECK = 'check'


class CustomerSegment(Enum):
    """Customer segmentation for business intelligence."""
    VIP = 'vip'
    PREMIUM = 'premium'
    REGULAR = 'regular'
    NEW = 'new'
    DORMANT = 'dormant'


@dataclass
class Money:
    """Value object for monetary amounts with currency handling."""
    amount: Decimal
    currency: str = 'GBP'
    
    def __post_init__(self):
        """Validate monetary amount."""
        if self.amount < 0:
            raise ValueError("Money amount cannot be negative")
        if not self.currency or len(self.currency) != 3:
            raise ValueError("Currency must be a 3-letter ISO code")
    
    def add(self, other: 'Money') -> 'Money':
        """Add two money amounts (same currency only)."""
        if self.currency != other.currency:
            raise ValueError("Cannot add different currencies")
        return Money(self.amount + other.amount, self.currency)
    
    def subtract(self, other: 'Money') -> 'Money':
        """Subtract two money amounts (same currency only)."""
        if self.currency != other.currency:
            raise ValueError("Cannot subtract different currencies")
        return Money(self.amount - other.amount, self.currency)
    
    def multiply(self, factor: Decimal) -> 'Money':
        """Multiply money by a factor."""
        return Money(self.amount * factor, self.currency)
    
    def is_zero(self) -> bool:
        """Check if amount is zero."""
        return self.amount == Decimal('0')
    
    def __str__(self) -> str:
        return f"{self.amount:.2f} {self.currency}"


@dataclass
class InvoiceLine:
    """Invoice line item - contains product and pricing details."""
    line_id: Optional[str] = None
    product_id: str = ''
    product_description: str = ''
    quantity: int = 0
    unit_price: Money = field(default_factory=lambda: Money(Decimal('0')))
    discount_percentage: Decimal = Decimal('0')
    tax_rate: Decimal = Decimal('0')
    
    def __post_init__(self):
        """Generate line ID if not provided and validate."""
        if not self.line_id:
            self.line_id = str(uuid4())
        self.validate()
    
    @property
    def line_amount(self) -> Money:
        """Calculate line amount before discount and tax."""
        return self.unit_price.multiply(Decimal(str(self.quantity)))
    
    @property
    def discount_amount(self) -> Money:
        """Calculate discount amount."""
        return self.line_amount.multiply(self.discount_percentage / 100)
    
    @property
    def taxable_amount(self) -> Money:
        """Calculate amount subject to tax (after discount)."""
        return self.line_amount.subtract(self.discount_amount)
    
    @property
    def tax_amount(self) -> Money:
        """Calculate tax amount."""
        return self.taxable_amount.multiply(self.tax_rate / 100)
    
    @property
    def net_amount(self) -> Money:
        """Calculate final net amount (after discount, with tax)."""
        return self.taxable_amount.add(self.tax_amount)
    
    def validate(self) -> None:
        """Validate business rules for invoice line."""
        if self.quantity <= 0:
            raise ValueError('Quantity must be positive')
        if self.unit_price.amount <= 0:
            raise ValueError('Unit price must be positive')
        if not (Decimal('0') <= self.discount_percentage <= Decimal('100')):
            raise ValueError('Discount percentage must be between 0 and 100')
        if self.tax_rate < 0:
            raise ValueError('Tax rate cannot be negative')
        if not self.product_id:
            raise ValueError('Product ID is required')
    
    def apply_bulk_discount(self, threshold_quantity: int, discount_rate: Decimal) -> None:
        """Apply bulk discount if quantity meets threshold."""
        if self.quantity >= threshold_quantity:
            self.discount_percentage = max(self.discount_percentage, discount_rate)
    
    def calculate_margin(self, cost_per_unit: Money) -> Decimal:
        """Calculate profit margin for this line."""
        if cost_per_unit.currency != self.unit_price.currency:
            raise ValueError("Cost and price currencies must match")
        
        if self.unit_price.amount <= cost_per_unit.amount:
            return Decimal('0')
        
        profit = self.unit_price.subtract(cost_per_unit)
        return (profit.amount / self.unit_price.amount) * 100


@dataclass
class Invoice:
    """Invoice aggregate root - contains all invoice data and business rules."""
    invoice_id: Optional[str] = None
    invoice_number: str = ''
    customer_id: str = ''
    store_id: str = ''
    invoice_date: date = field(default_factory=date.today)
    due_date: Optional[date] = None
    status: InvoiceStatus = InvoiceStatus.DRAFT
    payment_method: Optional[PaymentMethod] = None
    lines: List[InvoiceLine] = field(default_factory=list)
    notes: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.now)
    updated_at: datetime = field(default_factory=datetime.now)
    
    def __post_init__(self):
        """Initialize invoice with validation."""
        if not self.invoice_id:
            self.invoice_id = str(uuid4())
        if not self.due_date:
            # Default due date is 30 days from invoice date
            from datetime import timedelta
            self.due_date = self.invoice_date + timedelta(days=30)
        self.validate()
    
    @property
    def subtotal(self) -> Money:
        """Calculate invoice subtotal (before discounts and tax)."""
        if not self.lines:
            return Money(Decimal('0'))
        
        total = self.lines[0].line_amount
        for line in self.lines[1:]:
            total = total.add(line.line_amount)
        return total
    
    @property
    def total_discount(self) -> Money:
        """Calculate total discount amount."""
        if not self.lines:
            return Money(Decimal('0'))
        
        total = self.lines[0].discount_amount
        for line in self.lines[1:]:
            total = total.add(line.discount_amount)
        return total
    
    @property
    def total_tax(self) -> Money:
        """Calculate total tax amount."""
        if not self.lines:
            return Money(Decimal('0'))
        
        total = self.lines[0].tax_amount
        for line in self.lines[1:]:
            total = total.add(line.tax_amount)
        return total
    
    @property
    def total_amount(self) -> Money:
        """Calculate total invoice amount (final amount)."""
        if not self.lines:
            return Money(Decimal('0'))
        
        total = self.lines[0].net_amount
        for line in self.lines[1:]:
            total = total.add(line.net_amount)
        return total
    
    @property
    def line_count(self) -> int:
        """Get number of line items."""
        return len(self.lines)
    
    @property
    def is_overdue(self) -> bool:
        """Check if invoice is overdue."""
        return (self.status in [InvoiceStatus.PENDING] and 
                self.due_date < date.today())
    
    @property
    def is_high_value(self) -> bool:
        """Check if this is a high-value invoice."""
        return self.total_amount.amount >= Decimal('1000')
    
    def add_line(self, line: InvoiceLine) -> None:
        """Add line item to invoice with validation."""
        if self.status not in [InvoiceStatus.DRAFT]:
            raise ValueError(f"Cannot modify invoice in {self.status.value} status")
        
        line.validate()
        
        # Check for duplicate products
        existing_product_ids = [l.product_id for l in self.lines]
        if line.product_id in existing_product_ids:
            raise ValueError(f"Product {line.product_id} already exists in invoice")
        
        self.lines.append(line)
        self.updated_at = datetime.now()
    
    def remove_line(self, line_id: str) -> bool:
        """Remove line item from invoice."""
        if self.status not in [InvoiceStatus.DRAFT]:
            raise ValueError(f"Cannot modify invoice in {self.status.value} status")
        
        original_count = len(self.lines)
        self.lines = [l for l in self.lines if l.line_id != line_id]
        
        if len(self.lines) < original_count:
            self.updated_at = datetime.now()
            return True
        return False
    
    def update_line_quantity(self, line_id: str, new_quantity: int) -> bool:
        """Update quantity for a specific line item."""
        if self.status not in [InvoiceStatus.DRAFT]:
            raise ValueError(f"Cannot modify invoice in {self.status.value} status")
        
        if new_quantity <= 0:
            raise ValueError("Quantity must be positive")
        
        for line in self.lines:
            if line.line_id == line_id:
                line.quantity = new_quantity
                line.validate()  # Re-validate after change
                self.updated_at = datetime.now()
                return True
        return False
    
    def apply_invoice_discount(self, discount_percentage: Decimal) -> None:
        """Apply discount to all lines in the invoice."""
        if self.status not in [InvoiceStatus.DRAFT]:
            raise ValueError(f"Cannot modify invoice in {self.status.value} status")
        
        if not (Decimal('0') <= discount_percentage <= Decimal('50')):
            raise ValueError("Invoice discount must be between 0% and 50%")
        
        for line in self.lines:
            line.discount_percentage = max(line.discount_percentage, discount_percentage)
        
        self.updated_at = datetime.now()
    
    def finalize(self) -> None:
        """Finalize invoice - change status to pending."""
        if self.status != InvoiceStatus.DRAFT:
            raise ValueError(f"Cannot finalize invoice in {self.status.value} status")
        
        if not self.lines:
            raise ValueError("Cannot finalize invoice without line items")
        
        self.status = InvoiceStatus.PENDING
        self.updated_at = datetime.now()
    
    def mark_as_paid(self, payment_method: PaymentMethod, payment_date: Optional[date] = None) -> None:
        """Mark invoice as paid."""
        if self.status != InvoiceStatus.PENDING:
            raise ValueError(f"Cannot mark invoice as paid from {self.status.value} status")
        
        self.status = InvoiceStatus.PAID
        self.payment_method = payment_method
        self.updated_at = datetime.now()
    
    def cancel(self, reason: Optional[str] = None) -> None:
        """Cancel invoice."""
        if self.status not in [InvoiceStatus.DRAFT, InvoiceStatus.PENDING]:
            raise ValueError(f"Cannot cancel invoice in {self.status.value} status")
        
        self.status = InvoiceStatus.CANCELLED
        if reason:
            self.notes = f"Cancelled: {reason}"
        self.updated_at = datetime.now()
    
    def refund(self, reason: Optional[str] = None) -> None:
        """Process refund for paid invoice."""
        if self.status != InvoiceStatus.PAID:
            raise ValueError("Can only refund paid invoices")
        
        self.status = InvoiceStatus.REFUNDED
        if reason:
            self.notes = f"Refunded: {reason}"
        self.updated_at = datetime.now()
    
    def validate(self) -> None:
        """Validate invoice business rules."""
        if not self.invoice_number:
            raise ValueError("Invoice number is required")
        
        if not self.customer_id:
            raise ValueError("Customer ID is required")
        
        if not self.store_id:
            raise ValueError("Store ID is required")
        
        if self.due_date and self.due_date < self.invoice_date:
            raise ValueError("Due date cannot be before invoice date")
        
        if self.invoice_date > date.today():
            raise ValueError("Invoice date cannot be in the future")
        
        # Validate all lines
        for line in self.lines:
            line.validate()
    
    def get_payment_terms_days(self) -> int:
        """Calculate payment terms in days."""
        if not self.due_date:
            return 0
        return (self.due_date - self.invoice_date).days
    
    def calculate_aging_days(self) -> int:
        """Calculate how many days overdue (if applicable)."""
        if self.status != InvoiceStatus.PENDING:
            return 0
        
        if self.due_date >= date.today():
            return 0
        
        return (date.today() - self.due_date).days
    
    def to_summary_dict(self) -> Dict[str, Any]:
        """Convert to summary dictionary for analytics."""
        return {
            'invoice_id': self.invoice_id,
            'invoice_number': self.invoice_number,
            'customer_id': self.customer_id,
            'store_id': self.store_id,
            'invoice_date': self.invoice_date.isoformat(),
            'due_date': self.due_date.isoformat() if self.due_date else None,
            'status': self.status.value,
            'payment_method': self.payment_method.value if self.payment_method else None,
            'line_count': self.line_count,
            'subtotal': float(self.subtotal.amount),
            'total_discount': float(self.total_discount.amount),
            'total_tax': float(self.total_tax.amount),
            'total_amount': float(self.total_amount.amount),
            'currency': self.total_amount.currency,
            'is_high_value': self.is_high_value,
            'is_overdue': self.is_overdue,
            'aging_days': self.calculate_aging_days(),
            'created_at': self.created_at.isoformat(),
            'updated_at': self.updated_at.isoformat()
        }


@dataclass
class SalesAggregate:
    """Domain service for sales calculations and analytics."""
    invoices: List[Invoice]
    
    def total_revenue_by_period(self, start_date: date, end_date: date, 
                               statuses: Optional[List[InvoiceStatus]] = None) -> Money:
        """Calculate total revenue for a specific period."""
        if not statuses:
            statuses = [InvoiceStatus.PAID]
        
        filtered_invoices = [
            inv for inv in self.invoices
            if (start_date <= inv.invoice_date <= end_date and 
                inv.status in statuses)
        ]
        
        if not filtered_invoices:
            return Money(Decimal('0'))
        
        total = filtered_invoices[0].total_amount
        for inv in filtered_invoices[1:]:
            total = total.add(inv.total_amount)
        
        return total
    
    def revenue_by_customer(self, customer_id: str) -> Money:
        """Calculate total revenue for a specific customer."""
        customer_invoices = [
            inv for inv in self.invoices
            if (inv.customer_id == customer_id and 
                inv.status == InvoiceStatus.PAID)
        ]
        
        if not customer_invoices:
            return Money(Decimal('0'))
        
        total = customer_invoices[0].total_amount
        for inv in customer_invoices[1:]:
            total = total.add(inv.total_amount)
        
        return total
    
    def revenue_by_store(self, store_id: str) -> Money:
        """Calculate total revenue for a specific store."""
        store_invoices = [
            inv for inv in self.invoices
            if (inv.store_id == store_id and 
                inv.status == InvoiceStatus.PAID)
        ]
        
        if not store_invoices:
            return Money(Decimal('0'))
        
        total = store_invoices[0].total_amount
        for inv in store_invoices[1:]:
            total = total.add(inv.total_amount)
        
        return total
    
    def average_invoice_value(self) -> Money:
        """Calculate average invoice value for paid invoices."""
        paid_invoices = [inv for inv in self.invoices if inv.status == InvoiceStatus.PAID]
        
        if not paid_invoices:
            return Money(Decimal('0'))
        
        total_revenue = self.total_revenue_by_period(
            date.min, date.max, [InvoiceStatus.PAID]
        )
        
        average_amount = total_revenue.amount / len(paid_invoices)
        return Money(average_amount, total_revenue.currency)
    
    def payment_method_distribution(self) -> Dict[PaymentMethod, int]:
        """Get distribution of payment methods."""
        distribution = {}
        for invoice in self.invoices:
            if invoice.status == InvoiceStatus.PAID and invoice.payment_method:
                count = distribution.get(invoice.payment_method, 0)
                distribution[invoice.payment_method] = count + 1
        return distribution
    
    def overdue_invoices(self) -> List[Invoice]:
        """Get all overdue invoices."""
        return [inv for inv in self.invoices if inv.is_overdue]
    
    def high_value_invoices(self, threshold: Optional[Decimal] = None) -> List[Invoice]:
        """Get all high-value invoices."""
        if not threshold:
            threshold = Decimal('1000')
        
        return [inv for inv in self.invoices if inv.total_amount.amount >= threshold]
    
    def customer_lifetime_value(self, customer_id: str) -> Dict[str, Any]:
        """Calculate comprehensive customer lifetime value metrics."""
        customer_invoices = [inv for inv in self.invoices if inv.customer_id == customer_id]
        
        if not customer_invoices:
            return {
                'total_revenue': 0,
                'invoice_count': 0,
                'average_invoice_value': 0,
                'first_purchase_date': None,
                'last_purchase_date': None,
                'purchase_frequency_days': 0
            }
        
        paid_invoices = [inv for inv in customer_invoices if inv.status == InvoiceStatus.PAID]
        total_revenue = self.revenue_by_customer(customer_id)
        
        first_purchase = min(inv.invoice_date for inv in customer_invoices)
        last_purchase = max(inv.invoice_date for inv in customer_invoices)
        
        purchase_span_days = (last_purchase - first_purchase).days
        frequency = purchase_span_days / len(paid_invoices) if len(paid_invoices) > 1 else 0
        
        return {
            'customer_id': customer_id,
            'total_revenue': float(total_revenue.amount),
            'currency': total_revenue.currency,
            'invoice_count': len(paid_invoices),
            'average_invoice_value': float(total_revenue.amount / len(paid_invoices)) if paid_invoices else 0,
            'first_purchase_date': first_purchase.isoformat(),
            'last_purchase_date': last_purchase.isoformat(),
            'purchase_frequency_days': frequency,
            'total_transactions': len(customer_invoices)
        }
    
    def sales_performance_summary(self) -> Dict[str, Any]:
        """Generate comprehensive sales performance summary."""
        all_invoices = len(self.invoices)
        paid_invoices = len([inv for inv in self.invoices if inv.status == InvoiceStatus.PAID])
        cancelled_invoices = len([inv for inv in self.invoices if inv.status == InvoiceStatus.CANCELLED])
        overdue_count = len(self.overdue_invoices())
        high_value_count = len(self.high_value_invoices())
        
        total_revenue = self.total_revenue_by_period(date.min, date.max)
        avg_invoice = self.average_invoice_value()
        
        return {
            'total_invoices': all_invoices,
            'paid_invoices': paid_invoices,
            'cancelled_invoices': cancelled_invoices,
            'overdue_invoices': overdue_count,
            'high_value_invoices': high_value_count,
            'conversion_rate': (paid_invoices / all_invoices * 100) if all_invoices > 0 else 0,
            'total_revenue': float(total_revenue.amount),
            'average_invoice_value': float(avg_invoice.amount),
            'currency': total_revenue.currency,
            'payment_methods': self.payment_method_distribution()
        }