"""
Seed the warehouse with a few demo rows for end-to-end validation.

Usage (from repo root):
  poetry run python scripts/seed_data.py
"""
from __future__ import annotations

from datetime import datetime, date

from data_access.db import create_all, session_scope
from sqlmodel import select
from sqlalchemy import func
from data_access.models.star_schema import (
    DimDate,
    DimProduct,
    DimCustomer,
    DimCountry,
    DimInvoice,
    FactSale,
)


def yyyymmdd_key(d: date) -> int:
    return d.year * 10000 + d.month * 100 + d.day


def ensure_seed() -> None:
    create_all()

    with session_scope() as s:
        # If there are already sales, skip seeding
        existing = s.exec(select(func.count()).select_from(FactSale)).one()
        count = int(existing if isinstance(existing, int) else existing[0])
        if count > 0:
            print("Warehouse already has sales; skipping seed.")
            return

    # Insert small dimension set and a few fact rows
    with session_scope() as s:
        # Countries
        country_uk = DimCountry(name="United Kingdom")
        country_fr = DimCountry(name="France")
        s.add(country_uk)
        s.add(country_fr)
        s.flush()

        # Products
        prod1 = DimProduct(stock_code="85123A", description="White Hanging Heart T-Light Holder", category="Home")
        prod2 = DimProduct(stock_code="71053", description="White Metal Lantern", category="Home")
        s.add(prod1)
        s.add(prod2)
        s.flush()

        # Customers (optional)
        cust1 = DimCustomer(customer_id="CUST-0001", segment="Retail")
        s.add(cust1)
        s.flush()

        # Invoices
        inv1 = DimInvoice(invoice_no="536365", invoice_status="Completed", invoice_type="Sale")
        inv2 = DimInvoice(invoice_no="536366", invoice_status="Completed", invoice_type="Sale")
        s.add(inv1)
        s.add(inv2)
        s.flush()

        # Dates
        d1 = date(2010, 12, 1)
        d2 = date(2010, 12, 1)
        dimd1 = DimDate(
            date_key=yyyymmdd_key(d1), date=d1, year=d1.year, quarter=((d1.month - 1) // 3) + 1, month=d1.month, day=d1.day
        )
        dimd2 = DimDate(
            date_key=yyyymmdd_key(d2), date=d2, year=d2.year, quarter=((d2.month - 1) // 3) + 1, month=d2.month, day=d2.day
        )
        # Add dates if not present
        if not s.get(DimDate, dimd1.date_key):
            s.add(dimd1)
        if not s.get(DimDate, dimd2.date_key):
            s.add(dimd2)
        s.flush()

        # Fact rows
        ts1 = datetime(2010, 12, 1, 8, 26, 0)
        ts2 = datetime(2010, 12, 1, 8, 28, 0)

        sale1 = FactSale(
            date_key=yyyymmdd_key(d1),
            product_key=prod1.product_key,  # type: ignore[arg-type]
            customer_key=cust1.customer_key,  # type: ignore[arg-type]
            country_key=country_uk.country_key,  # type: ignore[arg-type]
            invoice_key=inv1.invoice_key,  # type: ignore[arg-type]
            quantity=6,
            unit_price=2.55,
            total=6 * 2.55,
            invoice_timestamp=ts1,
        )
        sale2 = FactSale(
            date_key=yyyymmdd_key(d2),
            product_key=prod2.product_key,  # type: ignore[arg-type]
            customer_key=None,
            country_key=country_fr.country_key,  # type: ignore[arg-type]
            invoice_key=inv2.invoice_key,  # type: ignore[arg-type]
            quantity=6,
            unit_price=3.39,
            total=6 * 3.39,
            invoice_timestamp=ts2,
        )
        s.add(sale1)
        s.add(sale2)
        # session_scope commits automatically

    print("Seeded warehouse with sample dimensions and 2 sales rows.")


if __name__ == "__main__":
    ensure_seed()
