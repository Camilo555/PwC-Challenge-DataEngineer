from datetime import datetime, date as Date
from typing import Optional

from sqlmodel import SQLModel, Field


class DimDate(SQLModel, table=True):
    __tablename__ = "dim_date"

    date_key: int | None = Field(default=None, primary_key=True)
    date: Date = Field(index=True)
    year: int
    quarter: int
    month: int
    day: int


class DimProduct(SQLModel, table=True):
    __tablename__ = "dim_product"

    product_key: int | None = Field(default=None, primary_key=True)
    stock_code: str = Field(index=True)
    description: Optional[str] = None
    category: Optional[str] = None


class DimCustomer(SQLModel, table=True):
    __tablename__ = "dim_customer"

    customer_key: int | None = Field(default=None, primary_key=True)
    customer_id: Optional[str] = Field(default=None, index=True)
    segment: Optional[str] = None


class DimCountry(SQLModel, table=True):
    __tablename__ = "dim_country"

    country_key: int | None = Field(default=None, primary_key=True)
    name: str = Field(index=True)


class DimInvoice(SQLModel, table=True):
    __tablename__ = "dim_invoice"

    invoice_key: int | None = Field(default=None, primary_key=True)
    invoice_no: str = Field(index=True)
    invoice_status: Optional[str] = None
    invoice_type: Optional[str] = None


class FactSale(SQLModel, table=True):
    __tablename__ = "fact_sale"

    sale_key: int | None = Field(default=None, primary_key=True)

    date_key: int = Field(foreign_key="dim_date.date_key", index=True)
    product_key: int = Field(foreign_key="dim_product.product_key", index=True)
    customer_key: Optional[int] = Field(default=None, foreign_key="dim_customer.customer_key", index=True)
    country_key: int = Field(foreign_key="dim_country.country_key", index=True)
    invoice_key: int = Field(foreign_key="dim_invoice.invoice_key", index=True)

    quantity: int
    unit_price: float
    total: float

    invoice_timestamp: datetime = Field(index=True)
