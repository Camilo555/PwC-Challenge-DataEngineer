from typing import List, Optional, Tuple
from datetime import datetime

from sqlmodel import select
from sqlalchemy import func, or_

from de_challenge.data_access.db import session_scope
from de_challenge.data_access.models.star_schema import (
    FactSale,
    DimProduct,
    DimCustomer,
    DimCountry,
    DimInvoice,
)
from de_challenge.api.v1.schemas.sales import SaleItem


class SalesRepository:
    def query_sales(
        self,
        *,
        date_from: Optional[str] = None,
        date_to: Optional[str] = None,
        product: Optional[str] = None,
        country: Optional[str] = None,
        page: int = 1,
        size: int = 20,
        sort: str = "invoice_date:desc",
    ) -> Tuple[List[SaleItem], int]:
        offset = (page - 1) * size

        # Base select with joins over dims needed for the API response
        base_stmt = (
            select(
                FactSale,
                DimProduct.stock_code,
                DimProduct.description,
                DimCustomer.customer_id,
                DimCountry.name.label("country"),
                DimInvoice.invoice_no,
            )
            .select_from(FactSale)
            .join(DimProduct, DimProduct.product_key == FactSale.product_key)
            .outerjoin(DimCustomer, DimCustomer.customer_key == FactSale.customer_key)
            .join(DimCountry, DimCountry.country_key == FactSale.country_key)
            .join(DimInvoice, DimInvoice.invoice_key == FactSale.invoice_key)
        )

        # Filters
        # Parse ISO8601 datetimes if provided
        if date_from:
            try:
                dt_from = datetime.fromisoformat(date_from)
                base_stmt = base_stmt.where(FactSale.invoice_timestamp >= dt_from)
            except ValueError:
                pass
        if date_to:
            try:
                dt_to = datetime.fromisoformat(date_to)
                base_stmt = base_stmt.where(FactSale.invoice_timestamp <= dt_to)
            except ValueError:
                pass
        if product:
            # match by stock_code exact or description ilike
            like = f"%{product.lower()}%"
            base_stmt = base_stmt.where(
                or_(
                    DimProduct.stock_code == product,
                    func.lower(DimProduct.description).like(like),
                )
            )
        if country:
            base_stmt = base_stmt.where(DimCountry.name == country)

        # Sorting
        sort_field, _, sort_dir = sort.partition(":")
        sort_dir = sort_dir or "desc"
        if sort_field == "invoice_date":
            order_col = FactSale.invoice_timestamp
        elif sort_field == "total":
            order_col = FactSale.total
        else:
            order_col = FactSale.invoice_timestamp
        if sort_dir.lower() == "asc":
            base_stmt = base_stmt.order_by(order_col.asc())
        else:
            base_stmt = base_stmt.order_by(order_col.desc())

        # Count on a filtered query without ORDER BY/columns to avoid driver issues
        count_base = (
            select(FactSale.sale_key)
            .select_from(FactSale)
            .join(DimProduct, DimProduct.product_key == FactSale.product_key)
            .outerjoin(DimCustomer, DimCustomer.customer_key == FactSale.customer_key)
            .join(DimCountry, DimCountry.country_key == FactSale.country_key)
            .join(DimInvoice, DimInvoice.invoice_key == FactSale.invoice_key)
        )
        if date_from:
            try:
                dt_from = datetime.fromisoformat(date_from)
                count_base = count_base.where(FactSale.invoice_timestamp >= dt_from)
            except ValueError:
                pass
        if date_to:
            try:
                dt_to = datetime.fromisoformat(date_to)
                count_base = count_base.where(FactSale.invoice_timestamp <= dt_to)
            except ValueError:
                pass
        if product:
            like = f"%{product.lower()}%"
            count_base = count_base.where(
                or_(
                    DimProduct.stock_code == product,
                    func.lower(DimProduct.description).like(like),
                )
            )
        if country:
            count_base = count_base.where(DimCountry.name == country)

        count_stmt = select(func.count()).select_from(count_base.subquery())

        items: List[SaleItem] = []
        total = 0
        with session_scope() as session:
            count_res = session.exec(count_stmt).one()
            try:
                total = int(count_res)
            except TypeError:
                total = int(count_res[0])
            rows = session.exec(base_stmt.offset(offset).limit(size)).all()

            for row in rows:
                fact: FactSale = row[0]
                stock_code = row[1]
                description = row[2]
                customer_id = row[3]
                country_name = row[4]
                invoice_no = row[5]

                items.append(
                    SaleItem(
                        invoice_no=invoice_no,
                        stock_code=stock_code,
                        description=description,
                        quantity=fact.quantity,
                        invoice_date=fact.invoice_timestamp.isoformat(),
                        unit_price=fact.unit_price,
                        customer_id=customer_id,
                        country=country_name,
                        total=round(float(fact.total), 2),
                        total_str=f"{round(float(fact.total), 2):.2f}",
                    )
                )
        return items, int(total)
