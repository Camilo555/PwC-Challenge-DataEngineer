"""
Test Data Factory for generating realistic test data for data engineering tests.
Provides comprehensive data generation capabilities for all layers of the medallion architecture.
"""
from __future__ import annotations

import random
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any

import numpy as np
import pandas as pd

try:
    from faker import Faker
    FAKER_AVAILABLE = True
except ImportError:
    FAKER_AVAILABLE = False
    Faker = None


class DataLayerType(Enum):
    """Data layer types in medallion architecture."""
    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


class DataQualityLevel(Enum):
    """Data quality levels for test scenarios."""
    PRISTINE = "pristine"      # Perfect data quality
    GOOD = "good"              # Minor quality issues
    FAIR = "fair"              # Moderate quality issues
    POOR = "poor"              # Major quality issues
    CORRUPTED = "corrupted"    # Severely corrupted data


@dataclass
class DataGenerationConfig:
    """Configuration for data generation."""
    num_records: int = 1000
    start_date: datetime = field(default_factory=lambda: datetime(2023, 1, 1))
    end_date: datetime = field(default_factory=lambda: datetime(2023, 12, 31))
    countries: list[str] = field(default_factory=lambda: [
        "USA", "UK", "Germany", "France", "Canada", "Australia",
        "Japan", "Brazil", "India", "China"
    ])
    data_quality: DataQualityLevel = DataQualityLevel.GOOD
    include_nulls: bool = True
    null_percentage: float = 0.02
    include_duplicates: bool = False
    duplicate_percentage: float = 0.01
    seed: int | None = None


class RetailDataFactory:
    """Factory for generating realistic retail sales data."""

    def __init__(self, config: DataGenerationConfig | None = None):
        """Initialize the data factory."""
        self.config = config or DataGenerationConfig()
        if self.config.seed:
            random.seed(self.config.seed)
            np.random.seed(self.config.seed)

        if FAKER_AVAILABLE:
            self.fake = Faker()
            if self.config.seed:
                Faker.seed(self.config.seed)
        else:
            self.fake = None

    def generate_bronze_layer_data(self) -> pd.DataFrame:
        """Generate bronze layer raw sales data."""
        records = []

        for i in range(self.config.num_records):
            # Generate base record
            record = {
                "invoice_id": self._generate_invoice_id(i),
                "customer_id": self._generate_customer_id(),
                "product_id": self._generate_product_id(),
                "country": self._generate_country(),
                "quantity": self._generate_quantity(),
                "unit_price": self._generate_unit_price(),
                "discount": self._generate_discount(),
                "invoice_date": self._generate_invoice_date(),
                "description": self._generate_product_description(),
                "stock_code": self._generate_stock_code(),
            }

            # Apply data quality issues based on configuration
            record = self._apply_data_quality_issues(record, i)
            records.append(record)

        df = pd.DataFrame(records)

        # Add duplicates if configured
        if self.config.include_duplicates:
            df = self._add_duplicates(df)

        return df

    def generate_silver_layer_data(self, bronze_df: pd.DataFrame | None = None) -> pd.DataFrame:
        """Generate silver layer cleaned and enriched data."""
        if bronze_df is None:
            bronze_df = self.generate_bronze_layer_data()

        # Start with bronze data
        silver_df = bronze_df.copy()

        # Add silver layer calculated fields
        silver_df["line_total"] = silver_df["quantity"] * silver_df["unit_price"]
        silver_df["discount_amount"] = silver_df["line_total"] * silver_df["discount"]
        silver_df["net_amount"] = silver_df["line_total"] - silver_df["discount_amount"]

        # Add metadata fields
        silver_df["created_at"] = datetime.now().isoformat()
        silver_df["updated_at"] = datetime.now().isoformat()
        silver_df["data_quality_score"] = np.random.uniform(0.85, 1.0, len(silver_df))

        # Add derived dimensions
        silver_df["year"] = pd.to_datetime(silver_df["invoice_date"]).dt.year
        silver_df["month"] = pd.to_datetime(silver_df["invoice_date"]).dt.month
        silver_df["quarter"] = pd.to_datetime(silver_df["invoice_date"]).dt.quarter
        silver_df["day_of_week"] = pd.to_datetime(silver_df["invoice_date"]).dt.day_name()

        # Add business categorizations
        silver_df["customer_segment"] = silver_df["customer_id"].apply(self._categorize_customer)
        silver_df["product_category"] = silver_df["product_id"].apply(self._categorize_product)
        silver_df["price_tier"] = silver_df["unit_price"].apply(self._categorize_price_tier)

        return silver_df

    def generate_gold_layer_dimensions(self) -> dict[str, pd.DataFrame]:
        """Generate gold layer dimension tables."""
        dimensions = {}

        # Customer dimension
        customer_ids = list(range(1000, 1000 + int(self.config.num_records * 0.3)))
        dimensions["dim_customer"] = pd.DataFrame({
            "customer_key": [f"CK_{i:06d}" for i in range(len(customer_ids))],
            "customer_id": customer_ids,
            "customer_name": [self._generate_customer_name() for _ in customer_ids],
            "email": [self._generate_email() for _ in customer_ids],
            "phone": [self._generate_phone() for _ in customer_ids],
            "registration_date": [self._generate_registration_date() for _ in customer_ids],
            "customer_segment": [self._categorize_customer(cid) for cid in customer_ids],
            "is_active": [random.choice([True, False]) for _ in customer_ids],
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat()
        })

        # Product dimension
        product_ids = list(range(2000, 2000 + int(self.config.num_records * 0.1)))
        dimensions["dim_product"] = pd.DataFrame({
            "product_key": [f"PK_{i:06d}" for i in range(len(product_ids))],
            "product_id": product_ids,
            "product_name": [self._generate_product_name() for _ in product_ids],
            "stock_code": [self._generate_stock_code() for _ in product_ids],
            "description": [self._generate_product_description() for _ in product_ids],
            "category": [self._categorize_product(pid) for pid in product_ids],
            "unit_cost": [round(random.uniform(5.0, 200.0), 2) for _ in product_ids],
            "is_active": [random.choice([True, False]) for _ in product_ids],
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat()
        })

        # Time dimension
        date_range = pd.date_range(
            start=self.config.start_date,
            end=self.config.end_date,
            freq='D'
        )
        dimensions["dim_time"] = pd.DataFrame({
            "time_key": [f"TK_{date.strftime('%Y%m%d')}" for date in date_range],
            "date": date_range.date,
            "year": date_range.year,
            "month": date_range.month,
            "day": date_range.day,
            "quarter": date_range.quarter,
            "day_of_week": date_range.day_name(),
            "day_of_month": date_range.day,
            "week_of_year": date_range.isocalendar().week,
            "is_weekend": date_range.day_of_week >= 5,
            "is_holiday": [self._is_holiday(date) for date in date_range],
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat()
        })

        # Geography dimension
        dimensions["dim_geography"] = pd.DataFrame({
            "geography_key": [f"GK_{i:03d}" for i in range(len(self.config.countries))],
            "country": self.config.countries,
            "country_code": [country[:2].upper() for country in self.config.countries],
            "continent": [self._get_continent(country) for country in self.config.countries],
            "population": [self._get_population(country) for country in self.config.countries],
            "gdp_per_capita": [random.randint(10000, 80000) for _ in self.config.countries],
            "currency": [self._get_currency(country) for country in self.config.countries],
            "timezone": [self._get_timezone(country) for country in self.config.countries],
            "created_at": datetime.now().isoformat(),
            "updated_at": datetime.now().isoformat()
        })

        return dimensions

    def generate_gold_layer_fact_table(self, silver_df: pd.DataFrame | None = None,
                                     dimensions: dict[str, pd.DataFrame] | None = None) -> pd.DataFrame:
        """Generate gold layer fact table with foreign keys."""
        if silver_df is None:
            silver_df = self.generate_silver_layer_data()

        if dimensions is None:
            dimensions = self.generate_gold_layer_dimensions()

        # Create lookup dictionaries for foreign keys
        customer_lookup = dict(zip(
            dimensions["dim_customer"]["customer_id"],
            dimensions["dim_customer"]["customer_key"], strict=False
        ))

        product_lookup = dict(zip(
            dimensions["dim_product"]["product_id"],
            dimensions["dim_product"]["product_key"], strict=False
        ))

        geography_lookup = dict(zip(
            dimensions["dim_geography"]["country"],
            dimensions["dim_geography"]["geography_key"], strict=False
        ))

        # Create time lookup
        time_lookup = dict(zip(
            pd.to_datetime(dimensions["dim_time"]["date"]).dt.strftime('%Y-%m-%d'),
            dimensions["dim_time"]["time_key"], strict=False
        ))

        # Build fact table
        fact_df = silver_df.copy()

        # Add foreign keys
        fact_df["customer_key"] = fact_df["customer_id"].map(customer_lookup)
        fact_df["product_key"] = fact_df["product_id"].map(product_lookup)
        fact_df["geography_key"] = fact_df["country"].map(geography_lookup)
        fact_df["time_key"] = pd.to_datetime(fact_df["invoice_date"]).dt.strftime('%Y-%m-%d').map(time_lookup)

        # Keep only fact table columns
        fact_columns = [
            "invoice_id", "customer_key", "product_key", "geography_key", "time_key",
            "quantity", "unit_price", "discount", "line_total", "discount_amount", "net_amount",
            "data_quality_score", "created_at", "updated_at"
        ]

        return fact_df[fact_columns]

    # Private helper methods
    def _generate_invoice_id(self, sequence: int) -> str:
        """Generate realistic invoice ID."""
        if self.config.data_quality == DataQualityLevel.CORRUPTED and random.random() < 0.1:
            return ""  # Return empty string for corrupted data
        return f"INV-{sequence + 1000:06d}"

    def _generate_customer_id(self) -> int:
        """Generate customer ID."""
        if self.config.data_quality == DataQualityLevel.CORRUPTED and random.random() < 0.05:
            return -1  # Invalid negative ID
        return random.randint(1000, 9999)

    def _generate_product_id(self) -> int:
        """Generate product ID."""
        if self.config.data_quality == DataQualityLevel.CORRUPTED and random.random() < 0.05:
            return 999999  # Out of range ID
        return random.randint(2000, 2999)

    def _generate_country(self) -> str:
        """Generate country name."""
        if self.config.data_quality == DataQualityLevel.CORRUPTED and random.random() < 0.03:
            return "InvalidCountry"  # Invalid country
        return random.choice(self.config.countries)

    def _generate_quantity(self) -> int:
        """Generate quantity."""
        if self.config.data_quality == DataQualityLevel.CORRUPTED and random.random() < 0.02:
            return random.choice([0, -1])  # Invalid quantities
        return random.randint(1, 50)

    def _generate_unit_price(self) -> float:
        """Generate unit price."""
        if self.config.data_quality == DataQualityLevel.CORRUPTED and random.random() < 0.02:
            return random.choice([0.0, -15.75])  # Invalid prices
        return round(random.uniform(1.0, 500.0), 2)

    def _generate_discount(self) -> float:
        """Generate discount percentage."""
        if self.config.data_quality == DataQualityLevel.CORRUPTED and random.random() < 0.02:
            return random.choice([1.5, -0.2])  # Invalid discounts
        return round(random.uniform(0.0, 0.3), 2)

    def _generate_invoice_date(self) -> str:
        """Generate invoice date."""
        if self.config.data_quality == DataQualityLevel.CORRUPTED and random.random() < 0.02:
            return "invalid-date"  # Invalid date

        date_range = (self.config.end_date - self.config.start_date).days
        random_date = self.config.start_date + timedelta(days=random.randint(0, date_range))
        return random_date.strftime("%Y-%m-%d")

    def _generate_product_description(self) -> str:
        """Generate product description."""
        descriptions = [
            "Handmade crafted item", "Premium quality product", "Limited edition item",
            "Vintage collectible", "Modern design piece", "Traditional handwork",
            "Exclusive designer item", "Artisan crafted piece", "Unique handmade item"
        ]
        return random.choice(descriptions)

    def _generate_stock_code(self) -> str:
        """Generate stock code."""
        letters = ''.join(random.choices('ABCDEFGHIJKLMNOPQRSTUVWXYZ', k=3))
        numbers = ''.join(random.choices('0123456789', k=3))
        return f"{letters}{numbers}"

    def _generate_customer_name(self) -> str:
        """Generate customer name."""
        if self.fake:
            return self.fake.name()

        first_names = ["John", "Jane", "Michael", "Sarah", "David", "Emma", "Chris", "Lisa"]
        last_names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller"]
        return f"{random.choice(first_names)} {random.choice(last_names)}"

    def _generate_email(self) -> str:
        """Generate email address."""
        if self.fake:
            return self.fake.email()

        domains = ["gmail.com", "yahoo.com", "outlook.com", "hotmail.com"]
        username = ''.join(random.choices('abcdefghijklmnopqrstuvwxyz0123456789', k=8))
        return f"{username}@{random.choice(domains)}"

    def _generate_phone(self) -> str:
        """Generate phone number."""
        if self.fake:
            return self.fake.phone_number()

        return f"+1-{random.randint(200, 999)}-{random.randint(200, 999)}-{random.randint(1000, 9999)}"

    def _generate_registration_date(self) -> str:
        """Generate customer registration date."""
        start_date = datetime(2020, 1, 1)
        end_date = self.config.start_date
        date_range = (end_date - start_date).days
        random_date = start_date + timedelta(days=random.randint(0, date_range))
        return random_date.strftime("%Y-%m-%d")

    def _generate_product_name(self) -> str:
        """Generate product name."""
        adjectives = ["Premium", "Deluxe", "Classic", "Modern", "Vintage", "Artisan", "Designer"]
        nouns = ["Mug", "Plate", "Vase", "Frame", "Lamp", "Cushion", "Candle", "Clock"]
        return f"{random.choice(adjectives)} {random.choice(nouns)}"

    def _categorize_customer(self, customer_id: int) -> str:
        """Categorize customer based on ID."""
        if customer_id % 10 < 3:
            return "Premium"
        elif customer_id % 10 < 7:
            return "Standard"
        else:
            return "Basic"

    def _categorize_product(self, product_id: int) -> str:
        """Categorize product based on ID."""
        categories = ["Home & Garden", "Electronics", "Clothing", "Books", "Sports", "Toys"]
        return categories[product_id % len(categories)]

    def _categorize_price_tier(self, price: float) -> str:
        """Categorize price into tiers."""
        if price >= 100:
            return "Premium"
        elif price >= 50:
            return "Mid-range"
        else:
            return "Budget"

    def _is_holiday(self, date: datetime) -> bool:
        """Check if date is a holiday (simplified)."""
        # Simple holiday detection (Christmas, New Year)
        return (date.month == 12 and date.day == 25) or (date.month == 1 and date.day == 1)

    def _get_continent(self, country: str) -> str:
        """Get continent for country."""
        continent_map = {
            "USA": "North America", "Canada": "North America",
            "UK": "Europe", "Germany": "Europe", "France": "Europe",
            "Australia": "Oceania", "Japan": "Asia", "China": "Asia", "India": "Asia",
            "Brazil": "South America"
        }
        return continent_map.get(country, "Unknown")

    def _get_population(self, country: str) -> int:
        """Get approximate population for country."""
        population_map = {
            "USA": 331000000, "China": 1440000000, "India": 1380000000,
            "Germany": 83000000, "UK": 67000000, "France": 65000000,
            "Canada": 38000000, "Australia": 25000000, "Japan": 126000000,
            "Brazil": 212000000
        }
        return population_map.get(country, 50000000)

    def _get_currency(self, country: str) -> str:
        """Get currency for country."""
        currency_map = {
            "USA": "USD", "Canada": "CAD", "UK": "GBP", "Germany": "EUR",
            "France": "EUR", "Australia": "AUD", "Japan": "JPY",
            "China": "CNY", "India": "INR", "Brazil": "BRL"
        }
        return currency_map.get(country, "USD")

    def _get_timezone(self, country: str) -> str:
        """Get timezone for country."""
        timezone_map = {
            "USA": "UTC-5", "Canada": "UTC-5", "UK": "UTC+0", "Germany": "UTC+1",
            "France": "UTC+1", "Australia": "UTC+10", "Japan": "UTC+9",
            "China": "UTC+8", "India": "UTC+5:30", "Brazil": "UTC-3"
        }
        return timezone_map.get(country, "UTC+0")

    def _apply_data_quality_issues(self, record: dict[str, Any], index: int) -> dict[str, Any]:
        """Apply data quality issues based on configuration."""
        if self.config.data_quality == DataQualityLevel.PRISTINE:
            return record

        # Apply nulls if configured
        if self.config.include_nulls and random.random() < self.config.null_percentage:
            # Randomly select a field to make null (except critical ones)
            nullable_fields = ["description", "stock_code", "discount"]
            if nullable_fields:
                field = random.choice(nullable_fields)
                record[field] = None

        return record

    def _add_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Add duplicate records based on configuration."""
        num_duplicates = int(len(df) * self.config.duplicate_percentage)
        if num_duplicates > 0:
            # Select random rows to duplicate
            duplicate_indices = random.sample(range(len(df)), min(num_duplicates, len(df)))
            duplicates = df.iloc[duplicate_indices].copy()

            # Slightly modify duplicates to make them realistic
            for idx in range(len(duplicates)):
                duplicates.iloc[idx, duplicates.columns.get_loc("invoice_id")] += "_DUP"

            df = pd.concat([df, duplicates], ignore_index=True)

        return df


class StreamingDataFactory:
    """Factory for generating streaming data for real-time testing."""

    def __init__(self, base_factory: RetailDataFactory):
        """Initialize streaming factory with base factory."""
        self.base_factory = base_factory

    def generate_streaming_batch(self, batch_size: int = 100,
                                batch_id: str | None = None) -> dict[str, Any]:
        """Generate a single streaming batch."""
        if batch_id is None:
            batch_id = f"batch_{uuid.uuid4().hex[:8]}"

        # Create smaller configuration for batch
        batch_config = DataGenerationConfig(
            num_records=batch_size,
            start_date=datetime.now() - timedelta(hours=1),
            end_date=datetime.now(),
            data_quality=self.base_factory.config.data_quality
        )

        # Temporarily override config
        original_config = self.base_factory.config
        self.base_factory.config = batch_config

        try:
            batch_data = self.base_factory.generate_bronze_layer_data()

            return {
                "batch_id": batch_id,
                "timestamp": datetime.now().isoformat(),
                "record_count": len(batch_data),
                "data": batch_data,
                "metadata": {
                    "data_quality": batch_config.data_quality.value,
                    "null_percentage": batch_config.null_percentage,
                    "generated_at": datetime.now().isoformat()
                }
            }
        finally:
            # Restore original config
            self.base_factory.config = original_config


# Test utilities and fixtures
def create_test_dataset(layer: DataLayerType, quality: DataQualityLevel = DataQualityLevel.GOOD,
                       size: int = 100) -> pd.DataFrame | dict[str, pd.DataFrame]:
    """Create test dataset for specified layer and quality."""
    config = DataGenerationConfig(
        num_records=size,
        data_quality=quality,
        seed=42  # For reproducible tests
    )

    factory = RetailDataFactory(config)

    if layer == DataLayerType.BRONZE:
        return factory.generate_bronze_layer_data()
    elif layer == DataLayerType.SILVER:
        return factory.generate_silver_layer_data()
    elif layer == DataLayerType.GOLD:
        dimensions = factory.generate_gold_layer_dimensions()
        fact_table = factory.generate_gold_layer_fact_table(dimensions=dimensions)
        return {
            "fact_sales": fact_table,
            **dimensions
        }
    else:
        raise ValueError(f"Unknown layer type: {layer}")


if __name__ == "__main__":
    print("Test Data Factory ready for use!")

    # Generate sample data
    config = DataGenerationConfig(num_records=10, seed=42)
    factory = RetailDataFactory(config)

    print("\nGenerating sample bronze data...")
    bronze_data = factory.generate_bronze_layer_data()
    print(f"Generated {len(bronze_data)} bronze records")

    print("\nGenerating sample silver data...")
    silver_data = factory.generate_silver_layer_data(bronze_data)
    print(f"Generated {len(silver_data)} silver records with {len(silver_data.columns)} columns")

    print("\nGenerating sample gold dimensions...")
    dimensions = factory.generate_gold_layer_dimensions()
    for dim_name, dim_df in dimensions.items():
        print(f"Generated {dim_name}: {len(dim_df)} records")

    print("\n✓ Test Data Factory ready for comprehensive testing")
