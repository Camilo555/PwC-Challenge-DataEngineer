"""
GraphQL DataLoader Implementation
Implements the DataLoader pattern to solve N+1 query problems.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any, Optional

from sqlmodel import Session, select
from strawberry.dataloader import DataLoader

from api.graphql.schemas import Country, Customer, Product
from core.logging import get_logger
from data_access.models.star_schema import DimCountry, DimCustomer, DimProduct

logger = get_logger(__name__)


class CustomerDataLoader(DataLoader[int, Optional[Customer]]):
    """DataLoader for batch loading customers by customer_key."""

    def __init__(self, session: Session):
        self.session = session
        super().__init__(load_fn=self.batch_load_customers)

    async def batch_load_customers(self, customer_keys: list[int]) -> list[Customer | None]:
        """Batch load customers by their keys."""
        try:
            # Remove None values and duplicates
            valid_keys = list({key for key in customer_keys if key is not None})

            if not valid_keys:
                return [None] * len(customer_keys)

            # Execute single query for all customer keys
            query = select(DimCustomer).where(DimCustomer.customer_key.in_(valid_keys))
            results = self.session.exec(query).all()

            # Create lookup dictionary
            customer_map = {
                customer.customer_key: Customer(
                    customer_key=customer.customer_key,
                    customer_id=customer.customer_id,
                    customer_segment=customer.customer_segment,
                    lifetime_value=float(customer.lifetime_value)
                    if customer.lifetime_value
                    else None,
                    total_orders=customer.total_orders,
                    total_spent=float(customer.total_spent) if customer.total_spent else None,
                    avg_order_value=float(customer.avg_order_value)
                    if customer.avg_order_value
                    else None,
                    recency_score=customer.recency_score,
                    frequency_score=customer.frequency_score,
                    monetary_score=customer.monetary_score,
                    rfm_segment=customer.rfm_segment,
                )
                for customer in results
            }

            # Return results in the same order as requested keys
            return [customer_map.get(key) for key in customer_keys]

        except Exception as e:
            logger.error(f"Error in CustomerDataLoader.batch_load_customers: {e}")
            # Return None for all requested keys on error
            return [None] * len(customer_keys)


class ProductDataLoader(DataLoader[int, Optional[Product]]):
    """DataLoader for batch loading products by product_key."""

    def __init__(self, session: Session):
        self.session = session
        super().__init__(load_fn=self.batch_load_products)

    async def batch_load_products(self, product_keys: list[int]) -> list[Product | None]:
        """Batch load products by their keys."""
        try:
            # Remove None values and duplicates
            valid_keys = list({key for key in product_keys if key is not None})

            if not valid_keys:
                return [None] * len(product_keys)

            # Execute single query for all product keys
            query = select(DimProduct).where(DimProduct.product_key.in_(valid_keys))
            results = self.session.exec(query).all()

            # Create lookup dictionary
            product_map = {
                product.product_key: Product(
                    product_key=product.product_key,
                    stock_code=product.stock_code,
                    description=product.description,
                    category=product.category,
                    subcategory=product.subcategory,
                    brand=product.brand,
                    unit_cost=float(product.unit_cost) if product.unit_cost else None,
                    recommended_price=float(product.recommended_price)
                    if product.recommended_price
                    else None,
                )
                for product in results
            }

            # Return results in the same order as requested keys
            return [product_map.get(key) for key in product_keys]

        except Exception as e:
            logger.error(f"Error in ProductDataLoader.batch_load_products: {e}")
            # Return None for all requested keys on error
            return [None] * len(product_keys)


class CountryDataLoader(DataLoader[int, Optional[Country]]):
    """DataLoader for batch loading countries by country_key."""

    def __init__(self, session: Session):
        self.session = session
        super().__init__(load_fn=self.batch_load_countries)

    async def batch_load_countries(self, country_keys: list[int]) -> list[Country | None]:
        """Batch load countries by their keys."""
        try:
            # Remove None values and duplicates
            valid_keys = list({key for key in country_keys if key is not None})

            if not valid_keys:
                return [None] * len(country_keys)

            # Execute single query for all country keys
            query = select(DimCountry).where(DimCountry.country_key.in_(valid_keys))
            results = self.session.exec(query).all()

            # Create lookup dictionary
            country_map = {
                country.country_key: Country(
                    country_key=country.country_key,
                    country_code=country.country_code,
                    country_name=country.country_name,
                    region=country.region,
                    continent=country.continent,
                    currency_code=country.currency_code,
                    gdp_per_capita=float(country.gdp_per_capita)
                    if country.gdp_per_capita
                    else None,
                    population=country.population,
                )
                for country in results
            }

            # Return results in the same order as requested keys
            return [country_map.get(key) for key in country_keys]

        except Exception as e:
            logger.error(f"Error in CountryDataLoader.batch_load_countries: {e}")
            # Return None for all requested keys on error
            return [None] * len(country_keys)


class SalesDataLoader(DataLoader[str, list[dict[str, Any]]]):
    """DataLoader for batch loading sales aggregation data."""

    def __init__(self, session: Session):
        self.session = session
        super().__init__(load_fn=self.batch_load_sales_data)

    async def batch_load_sales_data(self, query_keys: list[str]) -> list[list[dict[str, Any]]]:
        """
        Batch load aggregated sales data based on query parameters.
        query_keys format: "metric_type:date_range:filters_hash"
        """
        try:
            # Group similar queries to optimize database access
            query_groups = defaultdict(list)
            for i, key in enumerate(query_keys):
                # Parse query key to extract parameters
                parts = key.split(":")
                if len(parts) >= 2:
                    metric_type = parts[0]
                    date_range = parts[1]
                    query_groups[(metric_type, date_range)].append((i, key))

            # Initialize results array
            results = [[] for _ in range(len(query_keys))]

            # Process each group of similar queries
            for (metric_type, date_range), queries in query_groups.items():
                try:
                    # Execute optimized query for this group
                    # This is a simplified example - in practice, you'd implement
                    # specific aggregation queries based on the metric_type
                    group_results = await self._execute_aggregation_query(metric_type, date_range)

                    # Distribute results to appropriate positions
                    for idx, key in queries:
                        results[idx] = group_results

                except Exception as e:
                    logger.error(f"Error processing query group {metric_type}:{date_range}: {e}")
                    # Set empty results for failed queries
                    for idx, key in queries:
                        results[idx] = []

            return results

        except Exception as e:
            logger.error(f"Error in SalesDataLoader.batch_load_sales_data: {e}")
            return [[] for _ in range(len(query_keys))]

    async def _execute_aggregation_query(
        self, metric_type: str, date_range: str
    ) -> list[dict[str, Any]]:
        """Execute optimized aggregation query."""
        # This is a placeholder implementation
        # In a real implementation, you'd execute optimized SQL queries
        # based on the metric_type and date_range parameters
        return []


class DataLoaderFactory:
    """Factory for creating and managing DataLoader instances."""

    def __init__(self, session: Session):
        self.session = session
        self._customer_loader = None
        self._product_loader = None
        self._country_loader = None
        self._sales_loader = None

    def get_customer_loader(self) -> CustomerDataLoader:
        """Get or create customer data loader."""
        if self._customer_loader is None:
            self._customer_loader = CustomerDataLoader(self.session)
        return self._customer_loader

    def get_product_loader(self) -> ProductDataLoader:
        """Get or create product data loader."""
        if self._product_loader is None:
            self._product_loader = ProductDataLoader(self.session)
        return self._product_loader

    def get_country_loader(self) -> CountryDataLoader:
        """Get or create country data loader."""
        if self._country_loader is None:
            self._country_loader = CountryDataLoader(self.session)
        return self._country_loader

    def get_sales_loader(self) -> SalesDataLoader:
        """Get or create sales data loader."""
        if self._sales_loader is None:
            self._sales_loader = SalesDataLoader(self.session)
        return self._sales_loader

    def clear_all(self) -> None:
        """Clear all data loader caches."""
        if self._customer_loader:
            self._customer_loader.clear_all()
        if self._product_loader:
            self._product_loader.clear_all()
        if self._country_loader:
            self._country_loader.clear_all()
        if self._sales_loader:
            self._sales_loader.clear_all()


def get_dataloaders(session: Session) -> DataLoaderFactory:
    """Get DataLoader factory instance for the given session."""
    return DataLoaderFactory(session)
