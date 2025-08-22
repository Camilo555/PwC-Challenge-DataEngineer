
from sqlalchemy import func, or_
from sqlmodel import select

from data_access.db import session_scope
from data_access.models.star_schema import (
    DimCountry,
    DimCustomer,
    DimInvoice,
    DimProduct,
    FactSale,
)
from domain.entities.sale import Sale


class SalesRepository:
    def query_sales(
        self,
        *,
        date_from: str | None = None,
        date_to: str | None = None,
        product: str | None = None,
        country: str | None = None,
        page: int = 1,
        size: int = 20,
        sort: str = "invoice_date:desc",
    ) -> tuple[list[Sale], int]:
        offset = (page - 1) * size

        # Base select with joins over dims needed for the API response
        base_stmt = (
            select(  # type: ignore[call-overload]
                FactSale,
                DimProduct.stock_code,
                DimProduct.description,
                DimCustomer.customer_id,
                DimCountry.country_name.label("country"),  # type: ignore[attr-defined]
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
        # Note: Remove date filtering for now since we don't have invoice_timestamp in FactSale
        # Will be added back when we add date dimension relationship
        if product:
            # match by stock_code exact or description ilike
            like = f"%{product.lower()}%"
            base_stmt = base_stmt.where(
                or_(
                    DimProduct.stock_code == product,  # type: ignore[arg-type]
                    func.lower(DimProduct.description).like(like),
                )
            )
        if country:
            base_stmt = base_stmt.where(DimCountry.country_name == country)

        # Sorting
        sort_field, _, sort_dir = sort.partition(":")
        sort_dir = sort_dir or "desc"
        if sort_field == "invoice_date":
            order_col = FactSale.sale_id  # Use existing field for now
        elif sort_field == "total":
            order_col = FactSale.total_amount  # type: ignore[assignment]
        else:
            order_col = FactSale.sale_id
        if sort_dir.lower() == "asc":
            base_stmt = base_stmt.order_by(order_col.asc())  # type: ignore[union-attr]
        else:
            base_stmt = base_stmt.order_by(order_col.desc())  # type: ignore[union-attr]

        # Count on a filtered query without ORDER BY/columns to avoid driver issues
        count_base = (
            select(FactSale.sale_id)
            .select_from(FactSale)
            .join(DimProduct, DimProduct.product_key == FactSale.product_key)  # type: ignore[arg-type]
            .outerjoin(DimCustomer, DimCustomer.customer_key == FactSale.customer_key)  # type: ignore[arg-type]
            .join(DimCountry, DimCountry.country_key == FactSale.country_key)  # type: ignore[arg-type]
            .join(DimInvoice, DimInvoice.invoice_key == FactSale.invoice_key)  # type: ignore[arg-type]
        )
        # Note: Date filtering removed until we add proper date dimension relationship
        if product:
            like = f"%{product.lower()}%"
            count_base = count_base.where(
                or_(
                    DimProduct.stock_code == product,  # type: ignore[arg-type]
                    func.lower(DimProduct.description).like(like),
                )
            )
        if country:
            count_base = count_base.where(DimCountry.country_name == country)

        count_stmt = select(func.count()).select_from(count_base.subquery())

        items: list[SaleItem] = []
        total = 0
        with session_scope() as session:
            count_res = session.exec(count_stmt).one()
            try:
                total = int(count_res)
            except TypeError:
                total = int(count_res[0])  # type: ignore[index]
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
                        description=description or "",
                        quantity=fact.quantity,
                        invoice_date="2024-01-01T00:00:00",  # Placeholder until we add date relationship
                        unit_price=fact.unit_price,
                        customer_id=customer_id or "UNKNOWN",
                        country=country_name,
                        total=round(float(fact.total_amount), 2),
                        total_str=f"{round(float(fact.total_amount), 2):.2f}",
                    )
                )
        return items, int(total)
