
from pydantic import BaseModel, Field


class SaleItem(BaseModel):
    invoice_no: str
    stock_code: str
    description: str | None = None
    quantity: int
    invoice_date: str
    unit_price: float
    customer_id: str | None = None
    country: str | None = None
    total: float = Field(..., description="quantity * unit_price")
    total_str: str = Field(..., description="total formatted to 2 decimals")


class PaginatedSales(BaseModel):
    items: list[SaleItem]
    total: int
    page: int
    size: int
