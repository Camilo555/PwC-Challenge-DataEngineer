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
    week: int
    day_name: str
    is_weekend: bool = Field(default=False)
    is_holiday: bool = Field(default=False)


class DimProduct(SQLModel, table=True):
    __tablename__ = "dim_product"

    product_key: int | None = Field(default=None, primary_key=True)
    stock_code: str = Field(index=True)
    description: Optional[str] = None
    category: Optional[str] = None
    subcategory: Optional[str] = None
    brand: Optional[str] = None


class DimCustomer(SQLModel, table=True):
    __tablename__ = "dim_customer"

    customer_key: int | None = Field(default=None, primary_key=True)
    customer_id: Optional[str] = Field(default=None, index=True)
    customer_segment: Optional[str] = None
    registration_date: Optional[Date] = None
    lifetime_value: Optional[float] = None


class DimCountry(SQLModel, table=True):
    __tablename__ = "dim_country"

    country_key: int | None = Field(default=None, primary_key=True)
    country_code: Optional[str] = Field(default=None, index=True)
    country_name: str = Field(index=True)
    region: Optional[str] = None
    continent: Optional[str] = None


class DimInvoice(SQLModel, table=True):
    __tablename__ = "dim_invoice"

    invoice_key: int | None = Field(default=None, primary_key=True)
    invoice_no: str = Field(index=True)
    invoice_date: Optional[datetime] = None
    is_cancelled: bool = Field(default=False)
    payment_method: Optional[str] = None


class FactSale(SQLModel, table=True):
    __tablename__ = "fact_sale"

    sale_id: int | None = Field(default=None, primary_key=True)

    product_key: int = Field(foreign_key="dim_product.product_key", index=True)
    customer_key: Optional[int] = Field(default=None, foreign_key="dim_customer.customer_key", index=True)
    date_key: int = Field(foreign_key="dim_date.date_key", index=True)
    invoice_key: int = Field(foreign_key="dim_invoice.invoice_key", index=True)
    country_key: int = Field(foreign_key="dim_country.country_key", index=True)

    quantity: int
    unit_price: float
    total_amount: float
    discount_amount: Optional[float] = Field(default=0.0)
