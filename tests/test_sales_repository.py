from __future__ import annotations

from datetime import datetime, date

from de_challenge.data_access.db import create_all, session_scope
from de_challenge.data_access.models.star_schema import (
    DimDate, DimProduct, DimCustomer, DimCountry, DimInvoice, FactSale
)
from de_challenge.data_access.repositories.sales_repository import SalesRepository


def seed_minimal():
    create_all()
    with session_scope() as s:
        d = date(2024, 1, 2)
        dk = d.year * 10000 + d.month * 100 + d.day
        if not s.get(DimDate, dk):
            s.add(DimDate(
                date_key=dk, 
                date=d, 
                year=2024, 
                quarter=1, 
                month=1, 
                week=1,
                day_name="Tuesday",
                is_weekend=False,
                is_holiday=False
            ))
        prod = DimProduct(stock_code="10001", description="Test Widget", category="Electronics")
        cust = DimCustomer(customer_id="C001", customer_segment="Regular")
        ctry = DimCountry(country_name="United Kingdom", country_code="UK", region="Europe", continent="Europe")
        inv = DimInvoice(invoice_no="INV-1", is_cancelled=False)
        s.add_all([prod, cust, ctry, inv])
        s.flush()
        s.add(
            FactSale(
                product_key=prod.product_key,  # type: ignore[attr-defined]
                customer_key=cust.customer_key,  # type: ignore[attr-defined]
                date_key=dk,
                invoice_key=inv.invoice_key,  # type: ignore[attr-defined]
                country_key=ctry.country_key,  # type: ignore[attr-defined]
                quantity=3,
                unit_price=2.5,
                total_amount=7.5,
                discount_amount=0.0
            )
        )


def test_repository_query_basic():
    seed_minimal()
    repo = SalesRepository()
    items, total = repo.query_sales(size=10)
    assert total >= 1
    assert any(it.stock_code == "10001" for it in items)


def test_repository_filters_and_sort():
    repo = SalesRepository()
    # Filter by product code
    items, total = repo.query_sales(product="10001", size=5)
    assert total >= 1
    assert all(it.stock_code == "10001" for it in items)
    # Sort by total asc
    items2, _ = repo.query_sales(product="10001", size=5, sort="total:asc")
    assert items2
