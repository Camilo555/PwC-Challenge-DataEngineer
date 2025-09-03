"""
Comprehensive Test Data Factories
Enterprise-grade test data generation for all testing scenarios.
"""
from __future__ import annotations

import random
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, TypeVar

import numpy as np
import pandas as pd

try:
    from faker import Faker
    FAKER_AVAILABLE = True
except ImportError:
    FAKER_AVAILABLE = False
    Faker = None

T = TypeVar('T')


class TestScenario(Enum):
    """Test scenario types for different testing needs."""
    HAPPY_PATH = "happy_path"
    EDGE_CASE = "edge_case"
    ERROR_CONDITION = "error_condition"
    PERFORMANCE = "performance"
    SECURITY = "security"
    LOAD_TEST = "load_test"


class DataComplexity(Enum):
    """Data complexity levels."""
    SIMPLE = "simple"
    MODERATE = "moderate"
    COMPLEX = "complex"
    EXTREME = "extreme"


@dataclass
class TestDataConfig:
    """Configuration for test data generation."""
    scenario: TestScenario = TestScenario.HAPPY_PATH
    complexity: DataComplexity = DataComplexity.MODERATE
    size: int = 100
    seed: int | None = None
    include_nulls: bool = True
    null_percentage: float = 0.05
    include_duplicates: bool = False
    duplicate_percentage: float = 0.02
    include_outliers: bool = True
    outlier_percentage: float = 0.01
    time_range_days: int = 365
    custom_attributes: dict[str, Any] = field(default_factory=dict)


class BaseTestFactory(ABC):
    """Base class for all test factories."""

    def __init__(self, config: TestDataConfig | None = None):
        self.config = config or TestDataConfig()
        if self.config.seed:
            random.seed(self.config.seed)
            np.random.seed(self.config.seed)

        if FAKER_AVAILABLE:
            self.fake = Faker()
            if self.config.seed:
                Faker.seed(self.config.seed)
        else:
            self.fake = None

    @abstractmethod
    def generate_single_record(self) -> dict[str, Any]:
        """Generate a single test record."""
        pass

    def generate_batch(self, size: int | None = None) -> list[dict[str, Any]]:
        """Generate batch of test records."""
        batch_size = size or self.config.size
        return [self.generate_single_record() for _ in range(batch_size)]

    def generate_dataframe(self, size: int | None = None) -> pd.DataFrame:
        """Generate pandas DataFrame of test data."""
        records = self.generate_batch(size)
        df = pd.DataFrame(records)

        # Apply data quality issues
        if self.config.include_nulls:
            df = self._introduce_nulls(df)

        if self.config.include_duplicates:
            df = self._introduce_duplicates(df)

        if self.config.include_outliers:
            df = self._introduce_outliers(df)

        return df

    def _introduce_nulls(self, df: pd.DataFrame) -> pd.DataFrame:
        """Introduce null values based on configuration."""
        for column in df.columns:
            if df[column].dtype in [np.int64, np.float64, 'object']:
                null_mask = np.random.random(len(df)) < self.config.null_percentage
                df.loc[null_mask, column] = None
        return df

    def _introduce_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """Introduce duplicate rows based on configuration."""
        num_duplicates = int(len(df) * self.config.duplicate_percentage)
        if num_duplicates > 0:
            duplicate_indices = np.random.choice(len(df), num_duplicates, replace=False)
            duplicates = df.iloc[duplicate_indices].copy()
            df = pd.concat([df, duplicates], ignore_index=True)
        return df

    def _introduce_outliers(self, df: pd.DataFrame) -> pd.DataFrame:
        """Introduce outliers in numeric columns."""
        numeric_columns = df.select_dtypes(include=[np.number]).columns

        for column in numeric_columns:
            outlier_mask = np.random.random(len(df)) < self.config.outlier_percentage
            if outlier_mask.any():
                # Create extreme values (10x standard deviation from mean)
                mean = df[column].mean()
                std = df[column].std()
                outlier_multiplier = random.choice([-10, 10])
                outlier_value = mean + (std * outlier_multiplier)
                df.loc[outlier_mask, column] = outlier_value

        return df


class UserTestFactory(BaseTestFactory):
    """Factory for generating user test data."""

    def generate_single_record(self) -> dict[str, Any]:
        """Generate a single user record."""
        user_id = str(uuid.uuid4()) if self.config.scenario == TestScenario.HAPPY_PATH else self._generate_edge_case_id()

        base_record = {
            "user_id": user_id,
            "username": self._generate_username(),
            "email": self._generate_email(),
            "first_name": self._generate_first_name(),
            "last_name": self._generate_last_name(),
            "age": self._generate_age(),
            "created_at": self._generate_timestamp(),
            "is_active": self._generate_boolean(),
            "role": self._generate_user_role(),
            "last_login": self._generate_last_login(),
            "profile_completed": self._generate_boolean(0.7),  # 70% likely true
        }

        # Add complexity based on configuration
        if self.config.complexity in [DataComplexity.COMPLEX, DataComplexity.EXTREME]:
            base_record.update({
                "preferences": self._generate_user_preferences(),
                "permissions": self._generate_user_permissions(),
                "metadata": self._generate_user_metadata(),
                "social_profiles": self._generate_social_profiles(),
            })

        if self.config.complexity == DataComplexity.EXTREME:
            base_record.update({
                "activity_history": self._generate_activity_history(),
                "device_info": self._generate_device_info(),
                "location_data": self._generate_location_data(),
            })

        return base_record

    def _generate_edge_case_id(self) -> str:
        """Generate edge case user IDs."""
        edge_cases = [
            "",  # Empty ID
            "a" * 1000,  # Very long ID
            "user-with-special-chars!@#$%",
            "123456789",  # Numeric ID
            "null",  # String that could be confused with null
            "admin",  # Reserved word
        ]
        return random.choice(edge_cases) if self.config.scenario == TestScenario.EDGE_CASE else str(uuid.uuid4())

    def _generate_username(self) -> str:
        """Generate username based on scenario."""
        if self.fake:
            base_username = self.fake.user_name()
        else:
            base_username = f"user_{random.randint(1000, 9999)}"

        if self.config.scenario == TestScenario.EDGE_CASE:
            edge_cases = [
                "",  # Empty username
                "a",  # Single character
                "a" * 100,  # Very long username
                "user@domain.com",  # Email-like username
                "user with spaces",  # Spaces
                "用户名",  # Unicode characters
            ]
            return random.choice(edge_cases)

        return base_username

    def _generate_email(self) -> str:
        """Generate email address."""
        if self.fake:
            email = self.fake.email()
        else:
            email = f"user{random.randint(1000, 9999)}@example.com"

        if self.config.scenario == TestScenario.EDGE_CASE:
            edge_cases = [
                "",  # Empty email
                "invalid-email",  # Invalid format
                "test@",  # Incomplete
                "@example.com",  # Missing local part
                "a" * 100 + "@example.com",  # Very long local part
                "test+tag@example.com",  # Plus addressing
            ]
            return random.choice(edge_cases)

        return email

    def _generate_first_name(self) -> str:
        """Generate first name."""
        if self.fake:
            return self.fake.first_name()

        names = ["John", "Jane", "Michael", "Sarah", "David", "Emma", "Chris", "Lisa"]
        return random.choice(names)

    def _generate_last_name(self) -> str:
        """Generate last name."""
        if self.fake:
            return self.fake.last_name()

        names = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller"]
        return random.choice(names)

    def _generate_age(self) -> int:
        """Generate age with edge cases."""
        if self.config.scenario == TestScenario.EDGE_CASE:
            edge_cases = [-1, 0, 150, 999]  # Invalid ages
            if random.random() < 0.3:  # 30% chance of edge case
                return random.choice(edge_cases)

        return random.randint(18, 80)

    def _generate_timestamp(self) -> str:
        """Generate timestamp."""
        if self.config.scenario == TestScenario.EDGE_CASE:
            edge_cases = [
                "",  # Empty timestamp
                "invalid-date",  # Invalid format
                "2023-02-30",  # Invalid date
                "2023-13-01",  # Invalid month
            ]
            if random.random() < 0.2:  # 20% chance of edge case
                return random.choice(edge_cases)

        start_date = datetime.now() - timedelta(days=self.config.time_range_days)
        random_date = start_date + timedelta(days=random.randint(0, self.config.time_range_days))
        return random_date.isoformat()

    def _generate_boolean(self, true_probability: float = 0.5) -> bool:
        """Generate boolean with specified probability."""
        return random.random() < true_probability

    def _generate_user_role(self) -> str:
        """Generate user role."""
        roles = ["admin", "user", "moderator", "guest", "premium"]
        return random.choice(roles)

    def _generate_last_login(self) -> str | None:
        """Generate last login timestamp."""
        if random.random() < 0.1:  # 10% never logged in
            return None

        days_ago = random.randint(0, 30)
        last_login = datetime.now() - timedelta(days=days_ago)
        return last_login.isoformat()

    def _generate_user_preferences(self) -> dict[str, Any]:
        """Generate user preferences."""
        return {
            "theme": random.choice(["light", "dark", "auto"]),
            "language": random.choice(["en", "es", "fr", "de", "ja"]),
            "notifications": {
                "email": self._generate_boolean(0.8),
                "push": self._generate_boolean(0.6),
                "sms": self._generate_boolean(0.3),
            },
            "privacy": {
                "profile_public": self._generate_boolean(0.4),
                "show_activity": self._generate_boolean(0.6),
                "allow_messages": self._generate_boolean(0.7),
            }
        }

    def _generate_user_permissions(self) -> list[str]:
        """Generate user permissions."""
        all_permissions = [
            "read", "write", "delete", "admin", "moderate",
            "upload", "download", "share", "invite", "export"
        ]
        num_permissions = random.randint(1, min(5, len(all_permissions)))
        return random.sample(all_permissions, num_permissions)

    def _generate_user_metadata(self) -> dict[str, Any]:
        """Generate user metadata."""
        return {
            "signup_source": random.choice(["web", "mobile", "api", "import"]),
            "referrer": random.choice([None, "google", "facebook", "friend", "ad"]),
            "campaign": random.choice([None, "summer2023", "newuser", "premium"]),
            "tags": random.sample(["vip", "beta", "partner", "support", "trial"],
                                random.randint(0, 3)),
            "custom_fields": {
                f"field_{i}": f"value_{random.randint(1, 100)}"
                for i in range(random.randint(0, 5))
            }
        }

    def _generate_social_profiles(self) -> dict[str, str | None]:
        """Generate social profile links."""
        platforms = ["twitter", "linkedin", "github", "facebook", "instagram"]
        profiles = {}

        for platform in platforms:
            if random.random() < 0.3:  # 30% chance of having each platform
                profiles[platform] = f"https://{platform}.com/user{random.randint(1000, 9999)}"
            else:
                profiles[platform] = None

        return profiles

    def _generate_activity_history(self) -> list[dict[str, Any]]:
        """Generate user activity history."""
        num_activities = random.randint(0, 20)
        activities = []

        activity_types = ["login", "logout", "create", "update", "delete", "view", "download"]

        for _ in range(num_activities):
            activity = {
                "type": random.choice(activity_types),
                "timestamp": self._generate_timestamp(),
                "resource": f"resource_{random.randint(1, 100)}",
                "ip_address": f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}",
                "user_agent": "Mozilla/5.0 (compatible test agent)"
            }
            activities.append(activity)

        return activities

    def _generate_device_info(self) -> dict[str, Any]:
        """Generate device information."""
        return {
            "device_type": random.choice(["desktop", "mobile", "tablet"]),
            "os": random.choice(["Windows", "macOS", "Linux", "iOS", "Android"]),
            "browser": random.choice(["Chrome", "Firefox", "Safari", "Edge", "Opera"]),
            "screen_resolution": random.choice(["1920x1080", "1366x768", "1440x900", "375x667"]),
            "timezone": random.choice(["UTC", "EST", "PST", "CET", "JST"]),
        }

    def _generate_location_data(self) -> dict[str, Any]:
        """Generate location data."""
        return {
            "country": random.choice(["US", "UK", "CA", "DE", "FR", "JP", "AU"]),
            "region": random.choice(["North America", "Europe", "Asia", "Oceania"]),
            "city": self.fake.city() if self.fake else f"City_{random.randint(1, 100)}",
            "coordinates": {
                "latitude": round(random.uniform(-90, 90), 6),
                "longitude": round(random.uniform(-180, 180), 6),
            },
            "accuracy": random.randint(1, 1000),  # meters
        }


class SalesTestFactory(BaseTestFactory):
    """Factory for generating sales transaction test data."""

    def generate_single_record(self) -> dict[str, Any]:
        """Generate a single sales record."""
        base_record = {
            "transaction_id": self._generate_transaction_id(),
            "customer_id": self._generate_customer_id(),
            "product_id": self._generate_product_id(),
            "quantity": self._generate_quantity(),
            "unit_price": self._generate_unit_price(),
            "discount_rate": self._generate_discount_rate(),
            "transaction_date": self._generate_transaction_date(),
            "payment_method": self._generate_payment_method(),
            "currency": self._generate_currency(),
            "status": self._generate_transaction_status(),
        }

        # Calculate derived fields
        line_total = base_record["quantity"] * base_record["unit_price"]
        discount_amount = line_total * base_record["discount_rate"]
        base_record.update({
            "line_total": round(line_total, 2),
            "discount_amount": round(discount_amount, 2),
            "net_amount": round(line_total - discount_amount, 2),
        })

        # Add complexity based on configuration
        if self.config.complexity in [DataComplexity.COMPLEX, DataComplexity.EXTREME]:
            base_record.update({
                "shipping_info": self._generate_shipping_info(),
                "tax_info": self._generate_tax_info(),
                "promotional_codes": self._generate_promotional_codes(),
            })

        if self.config.complexity == DataComplexity.EXTREME:
            base_record.update({
                "payment_details": self._generate_payment_details(),
                "fulfillment_history": self._generate_fulfillment_history(),
                "customer_notes": self._generate_customer_notes(),
                "internal_notes": self._generate_internal_notes(),
            })

        return base_record

    def _generate_transaction_id(self) -> str:
        """Generate transaction ID."""
        if self.config.scenario == TestScenario.EDGE_CASE:
            edge_cases = ["", "TXN-" + "0" * 50, "INVALID_ID", str(uuid.uuid4()) + "_DUPLICATE"]
            if random.random() < 0.2:
                return random.choice(edge_cases)

        return f"TXN-{random.randint(100000, 999999)}-{int(time.time())}"

    def _generate_customer_id(self) -> str:
        """Generate customer ID."""
        return f"CUST-{random.randint(1000, 99999)}"

    def _generate_product_id(self) -> str:
        """Generate product ID."""
        return f"PROD-{random.randint(100, 9999)}"

    def _generate_quantity(self) -> int:
        """Generate quantity with edge cases."""
        if self.config.scenario == TestScenario.EDGE_CASE:
            edge_cases = [0, -1, 999999]
            if random.random() < 0.1:
                return random.choice(edge_cases)

        return random.randint(1, 10)

    def _generate_unit_price(self) -> float:
        """Generate unit price with edge cases."""
        if self.config.scenario == TestScenario.EDGE_CASE:
            edge_cases = [0.0, -1.0, 999999.99]
            if random.random() < 0.1:
                return random.choice(edge_cases)

        return round(random.uniform(1.0, 500.0), 2)

    def _generate_discount_rate(self) -> float:
        """Generate discount rate."""
        if self.config.scenario == TestScenario.EDGE_CASE:
            edge_cases = [-0.1, 1.1, 999.0]  # Invalid discount rates
            if random.random() < 0.1:
                return random.choice(edge_cases)

        return round(random.uniform(0.0, 0.3), 2)

    def _generate_transaction_date(self) -> str:
        """Generate transaction date."""
        if self.config.scenario == TestScenario.EDGE_CASE:
            edge_cases = ["", "invalid-date", "2023-02-30", "2023-13-01"]
            if random.random() < 0.1:
                return random.choice(edge_cases)

        days_ago = random.randint(0, self.config.time_range_days)
        transaction_date = datetime.now() - timedelta(days=days_ago)
        return transaction_date.isoformat()

    def _generate_payment_method(self) -> str:
        """Generate payment method."""
        methods = ["credit_card", "debit_card", "paypal", "bank_transfer", "cash", "crypto"]
        return random.choice(methods)

    def _generate_currency(self) -> str:
        """Generate currency code."""
        currencies = ["USD", "EUR", "GBP", "JPY", "CAD", "AUD", "CHF"]
        return random.choice(currencies)

    def _generate_transaction_status(self) -> str:
        """Generate transaction status."""
        statuses = ["completed", "pending", "failed", "cancelled", "refunded"]
        weights = [0.7, 0.15, 0.05, 0.05, 0.05]  # Most transactions are completed
        return random.choices(statuses, weights=weights)[0]

    def _generate_shipping_info(self) -> dict[str, Any]:
        """Generate shipping information."""
        return {
            "method": random.choice(["standard", "express", "overnight", "pickup"]),
            "cost": round(random.uniform(0.0, 50.0), 2),
            "estimated_days": random.randint(1, 14),
            "tracking_number": f"TRACK-{random.randint(100000000, 999999999)}",
            "address": {
                "street": f"{random.randint(1, 9999)} Main St",
                "city": self.fake.city() if self.fake else f"City_{random.randint(1, 100)}",
                "state": random.choice(["CA", "NY", "TX", "FL", "IL"]),
                "zip_code": f"{random.randint(10000, 99999)}",
                "country": "US"
            }
        }

    def _generate_tax_info(self) -> dict[str, Any]:
        """Generate tax information."""
        return {
            "tax_rate": round(random.uniform(0.05, 0.15), 4),
            "tax_amount": round(random.uniform(1.0, 50.0), 2),
            "tax_jurisdiction": random.choice(["state", "federal", "local", "international"]),
            "tax_exemption": random.choice([None, "non_profit", "government", "resale"])
        }

    def _generate_promotional_codes(self) -> list[str]:
        """Generate promotional codes."""
        codes = ["SAVE10", "WELCOME20", "SUMMER2023", "FIRSTBUY", "LOYAL15"]
        num_codes = random.randint(0, 2)
        return random.sample(codes, num_codes)

    def _generate_payment_details(self) -> dict[str, Any]:
        """Generate payment details."""
        return {
            "processor": random.choice(["stripe", "paypal", "square", "authorize.net"]),
            "transaction_fee": round(random.uniform(0.30, 5.0), 2),
            "authorization_code": f"AUTH-{random.randint(100000, 999999)}",
            "risk_score": random.randint(1, 100),
            "fraud_check": random.choice(["passed", "flagged", "failed"]),
        }

    def _generate_fulfillment_history(self) -> list[dict[str, Any]]:
        """Generate fulfillment history."""
        statuses = ["ordered", "processing", "shipped", "delivered"]
        history = []

        for status in statuses:
            if random.random() < 0.8:  # 80% chance of each status
                history.append({
                    "status": status,
                    "timestamp": self._generate_transaction_date(),
                    "location": self.fake.city() if self.fake else f"City_{random.randint(1, 100)}",
                    "notes": f"Status updated to {status}"
                })

        return history

    def _generate_customer_notes(self) -> str | None:
        """Generate customer notes."""
        notes = [
            "Please deliver to front door",
            "Gift wrap requested",
            "Urgent delivery needed",
            "Handle with care",
            "Leave at reception"
        ]
        return random.choice(notes) if random.random() < 0.3 else None

    def _generate_internal_notes(self) -> str | None:
        """Generate internal notes."""
        notes = [
            "VIP customer",
            "Bulk order discount applied",
            "Inventory reserved",
            "Special handling required",
            "Price match applied"
        ]
        return random.choice(notes) if random.random() < 0.2 else None


class ProductTestFactory(BaseTestFactory):
    """Factory for generating product test data."""

    def generate_single_record(self) -> dict[str, Any]:
        """Generate a single product record."""
        base_record = {
            "product_id": self._generate_product_id(),
            "name": self._generate_product_name(),
            "description": self._generate_product_description(),
            "category": self._generate_category(),
            "brand": self._generate_brand(),
            "price": self._generate_price(),
            "cost": self._generate_cost(),
            "weight": self._generate_weight(),
            "dimensions": self._generate_dimensions(),
            "color": self._generate_color(),
            "material": self._generate_material(),
            "stock_quantity": self._generate_stock_quantity(),
            "reorder_level": self._generate_reorder_level(),
            "supplier_id": self._generate_supplier_id(),
            "created_date": self._generate_created_date(),
            "is_active": self._generate_is_active(),
            "rating": self._generate_rating(),
            "review_count": self._generate_review_count(),
        }

        # Add complexity based on configuration
        if self.config.complexity in [DataComplexity.COMPLEX, DataComplexity.EXTREME]:
            base_record.update({
                "attributes": self._generate_product_attributes(),
                "variants": self._generate_product_variants(),
                "categories": self._generate_product_categories(),
                "tags": self._generate_product_tags(),
                "images": self._generate_product_images(),
            })

        if self.config.complexity == DataComplexity.EXTREME:
            base_record.update({
                "seo": self._generate_seo_data(),
                "analytics": self._generate_analytics_data(),
                "compliance": self._generate_compliance_data(),
                "seasonal_data": self._generate_seasonal_data(),
            })

        return base_record

    def _generate_product_id(self) -> str:
        """Generate product ID."""
        return f"PROD-{random.randint(100000, 999999)}"

    def _generate_product_name(self) -> str:
        """Generate product name."""
        adjectives = ["Premium", "Deluxe", "Classic", "Modern", "Vintage", "Smart", "Eco"]
        nouns = ["Widget", "Gadget", "Tool", "Device", "Accessory", "Component", "System"]
        return f"{random.choice(adjectives)} {random.choice(nouns)} {random.randint(100, 999)}"

    def _generate_product_description(self) -> str:
        """Generate product description."""
        descriptions = [
            "High-quality product designed for everyday use.",
            "Professional-grade solution for demanding applications.",
            "Innovative design meets practical functionality.",
            "Eco-friendly option with sustainable materials.",
            "Compact and lightweight for portability.",
        ]
        return random.choice(descriptions)

    def _generate_category(self) -> str:
        """Generate product category."""
        categories = ["Electronics", "Home & Garden", "Clothing", "Sports", "Books", "Toys", "Health"]
        return random.choice(categories)

    def _generate_brand(self) -> str:
        """Generate brand name."""
        brands = ["TechCorp", "HomeLife", "SportsPro", "EcoGreen", "SmartSolutions", "QualityFirst"]
        return random.choice(brands)

    def _generate_price(self) -> float:
        """Generate product price."""
        return round(random.uniform(10.0, 1000.0), 2)

    def _generate_cost(self) -> float:
        """Generate product cost (typically 60-80% of price)."""
        price = self._generate_price()
        cost_ratio = random.uniform(0.6, 0.8)
        return round(price * cost_ratio, 2)

    def _generate_weight(self) -> float:
        """Generate product weight in kg."""
        return round(random.uniform(0.1, 50.0), 2)

    def _generate_dimensions(self) -> dict[str, float]:
        """Generate product dimensions."""
        return {
            "length": round(random.uniform(1, 100), 1),
            "width": round(random.uniform(1, 100), 1),
            "height": round(random.uniform(1, 100), 1),
        }

    def _generate_color(self) -> str:
        """Generate product color."""
        colors = ["Black", "White", "Red", "Blue", "Green", "Yellow", "Gray", "Silver", "Gold"]
        return random.choice(colors)

    def _generate_material(self) -> str:
        """Generate product material."""
        materials = ["Plastic", "Metal", "Wood", "Glass", "Fabric", "Ceramic", "Composite"]
        return random.choice(materials)

    def _generate_stock_quantity(self) -> int:
        """Generate stock quantity."""
        return random.randint(0, 1000)

    def _generate_reorder_level(self) -> int:
        """Generate reorder level."""
        return random.randint(10, 100)

    def _generate_supplier_id(self) -> str:
        """Generate supplier ID."""
        return f"SUPP-{random.randint(1000, 9999)}"

    def _generate_created_date(self) -> str:
        """Generate product creation date."""
        days_ago = random.randint(0, self.config.time_range_days * 2)  # Products can be older
        created_date = datetime.now() - timedelta(days=days_ago)
        return created_date.isoformat()

    def _generate_is_active(self) -> bool:
        """Generate active status."""
        return random.random() < 0.9  # 90% of products are active

    def _generate_rating(self) -> float:
        """Generate product rating."""
        return round(random.uniform(1.0, 5.0), 1)

    def _generate_review_count(self) -> int:
        """Generate review count."""
        return random.randint(0, 500)

    def _generate_product_attributes(self) -> dict[str, Any]:
        """Generate product attributes."""
        return {
            "warranty_years": random.randint(1, 5),
            "energy_rating": random.choice(["A++", "A+", "A", "B", "C"]),
            "assembly_required": random.choice([True, False]),
            "age_group": random.choice(["Adult", "Child", "Teen", "All Ages"]),
            "skill_level": random.choice(["Beginner", "Intermediate", "Advanced"]),
        }

    def _generate_product_variants(self) -> list[dict[str, Any]]:
        """Generate product variants."""
        variants = []
        num_variants = random.randint(1, 5)

        for i in range(num_variants):
            variant = {
                "variant_id": f"VAR-{random.randint(1000, 9999)}",
                "name": f"Variant {i+1}",
                "sku": f"SKU-{random.randint(100000, 999999)}",
                "price_modifier": round(random.uniform(-50.0, 50.0), 2),
                "attributes": {
                    "size": random.choice(["Small", "Medium", "Large", "XL"]),
                    "color": self._generate_color(),
                }
            }
            variants.append(variant)

        return variants

    def _generate_product_categories(self) -> list[str]:
        """Generate multiple categories for a product."""
        all_categories = [
            "Electronics", "Home & Garden", "Clothing", "Sports",
            "Books", "Toys", "Health", "Automotive", "Beauty"
        ]
        num_categories = random.randint(1, 3)
        return random.sample(all_categories, num_categories)

    def _generate_product_tags(self) -> list[str]:
        """Generate product tags."""
        all_tags = [
            "bestseller", "new", "sale", "featured", "premium",
            "eco-friendly", "handmade", "limited-edition", "trending"
        ]
        num_tags = random.randint(0, 5)
        return random.sample(all_tags, num_tags)

    def _generate_product_images(self) -> list[dict[str, Any]]:
        """Generate product images."""
        images = []
        num_images = random.randint(1, 8)

        for i in range(num_images):
            image = {
                "url": f"https://images.example.com/product_{random.randint(1000, 9999)}_{i}.jpg",
                "alt_text": f"Product image {i+1}",
                "is_primary": i == 0,
                "sort_order": i,
            }
            images.append(image)

        return images

    def _generate_seo_data(self) -> dict[str, Any]:
        """Generate SEO data."""
        return {
            "meta_title": f"Buy {self._generate_product_name()} - Best Price",
            "meta_description": f"Shop {self._generate_product_name()} with free shipping and returns.",
            "keywords": random.sample(["quality", "affordable", "premium", "best", "top"], 3),
            "slug": f"product-{random.randint(1000, 9999)}",
        }

    def _generate_analytics_data(self) -> dict[str, Any]:
        """Generate analytics data."""
        return {
            "views_total": random.randint(0, 10000),
            "views_this_month": random.randint(0, 1000),
            "conversion_rate": round(random.uniform(0.01, 0.15), 4),
            "bounce_rate": round(random.uniform(0.20, 0.80), 4),
            "avg_time_on_page": random.randint(30, 300),  # seconds
        }

    def _generate_compliance_data(self) -> dict[str, Any]:
        """Generate compliance data."""
        return {
            "certifications": random.sample(["CE", "FCC", "UL", "ISO"], random.randint(0, 3)),
            "safety_warnings": random.sample(
                ["Keep away from children", "Do not expose to water", "Read manual before use"],
                random.randint(0, 2)
            ),
            "restricted_countries": random.sample(["CN", "RU", "IR"], random.randint(0, 2)),
        }

    def _generate_seasonal_data(self) -> dict[str, Any]:
        """Generate seasonal data."""
        return {
            "peak_seasons": random.sample(["spring", "summer", "fall", "winter"], random.randint(1, 2)),
            "holiday_relevance": random.sample(
                ["Christmas", "Valentine's Day", "Halloween", "Easter"],
                random.randint(0, 2)
            ),
            "seasonal_price_modifier": round(random.uniform(0.8, 1.3), 2),
        }


class APITestFactory:
    """Factory for generating API test data."""

    def __init__(self, config: TestDataConfig | None = None):
        self.config = config or TestDataConfig()

    def generate_api_request(self, endpoint: str = "/api/test") -> dict[str, Any]:
        """Generate API request data."""
        return {
            "method": random.choice(["GET", "POST", "PUT", "DELETE", "PATCH"]),
            "endpoint": endpoint,
            "headers": self._generate_headers(),
            "query_params": self._generate_query_params(),
            "body": self._generate_request_body(),
            "timestamp": datetime.now().isoformat(),
        }

    def generate_api_response(self, status_code: int = 200) -> dict[str, Any]:
        """Generate API response data."""
        return {
            "status_code": status_code,
            "headers": self._generate_response_headers(),
            "body": self._generate_response_body(status_code),
            "response_time_ms": random.randint(10, 2000),
            "timestamp": datetime.now().isoformat(),
        }

    def _generate_headers(self) -> dict[str, str]:
        """Generate request headers."""
        base_headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "TestClient/1.0",
        }

        if random.random() < 0.7:  # 70% chance of auth header
            base_headers["Authorization"] = f"Bearer {uuid.uuid4()}"

        if random.random() < 0.3:  # 30% chance of custom headers
            base_headers.update({
                "X-Request-ID": str(uuid.uuid4()),
                "X-Client-Version": "1.0.0",
                "X-Feature-Flag": random.choice(["enabled", "disabled"]),
            })

        return base_headers

    def _generate_query_params(self) -> dict[str, Any]:
        """Generate query parameters."""
        params = {}

        if random.random() < 0.5:  # 50% chance of pagination
            params.update({
                "page": random.randint(1, 10),
                "limit": random.choice([10, 25, 50, 100]),
            })

        if random.random() < 0.4:  # 40% chance of filters
            params.update({
                "status": random.choice(["active", "inactive", "pending"]),
                "category": random.choice(["electronics", "books", "clothing"]),
                "sort": random.choice(["name", "date", "price"]),
                "order": random.choice(["asc", "desc"]),
            })

        return params

    def _generate_request_body(self) -> dict[str, Any] | None:
        """Generate request body."""
        if random.random() < 0.6:  # 60% chance of having a body
            return {
                "id": str(uuid.uuid4()),
                "name": f"Test Item {random.randint(1, 1000)}",
                "value": random.randint(1, 100),
                "active": random.choice([True, False]),
                "metadata": {
                    "source": "test",
                    "version": "1.0",
                    "tags": random.sample(["test", "demo", "sample"], random.randint(1, 2)),
                }
            }
        return None

    def _generate_response_headers(self) -> dict[str, str]:
        """Generate response headers."""
        return {
            "Content-Type": "application/json",
            "X-Response-Time": f"{random.randint(1, 100)}ms",
            "X-Request-ID": str(uuid.uuid4()),
            "Cache-Control": random.choice(["no-cache", "max-age=3600", "public"]),
        }

    def _generate_response_body(self, status_code: int) -> dict[str, Any]:
        """Generate response body based on status code."""
        if status_code == 200:
            return {
                "status": "success",
                "data": {
                    "id": str(uuid.uuid4()),
                    "result": "Operation completed successfully",
                    "timestamp": datetime.now().isoformat(),
                }
            }
        elif status_code == 400:
            return {
                "status": "error",
                "error": {
                    "code": "VALIDATION_ERROR",
                    "message": "Invalid request parameters",
                    "details": ["Field 'name' is required", "Field 'value' must be positive"]
                }
            }
        elif status_code == 404:
            return {
                "status": "error",
                "error": {
                    "code": "NOT_FOUND",
                    "message": "Resource not found",
                    "resource_id": str(uuid.uuid4())
                }
            }
        elif status_code == 500:
            return {
                "status": "error",
                "error": {
                    "code": "INTERNAL_ERROR",
                    "message": "An unexpected error occurred",
                    "trace_id": str(uuid.uuid4())
                }
            }
        else:
            return {
                "status": "unknown",
                "message": f"Status code {status_code}",
                "timestamp": datetime.now().isoformat(),
            }


# Factory registry for easy access
FACTORY_REGISTRY = {
    "user": UserTestFactory,
    "sales": SalesTestFactory,
    "product": ProductTestFactory,
    "api": APITestFactory,
}


def get_factory(factory_type: str, config: TestDataConfig | None = None):
    """Get factory instance by type."""
    if factory_type not in FACTORY_REGISTRY:
        raise ValueError(f"Unknown factory type: {factory_type}")

    factory_class = FACTORY_REGISTRY[factory_type]
    return factory_class(config)


def generate_test_dataset(
    factory_type: str,
    scenario: TestScenario = TestScenario.HAPPY_PATH,
    complexity: DataComplexity = DataComplexity.MODERATE,
    size: int = 100,
    **kwargs
) -> pd.DataFrame | list[dict[str, Any]]:
    """Generate test dataset using specified factory."""
    config = TestDataConfig(
        scenario=scenario,
        complexity=complexity,
        size=size,
        **kwargs
    )

    factory = get_factory(factory_type, config)

    if factory_type == "api":
        # API factory returns individual requests/responses
        return factory.generate_batch(size)
    else:
        # Other factories return DataFrames
        return factory.generate_dataframe(size)


if __name__ == "__main__":
    print("Comprehensive Test Factories ready!")

    # Example usage
    config = TestDataConfig(
        scenario=TestScenario.HAPPY_PATH,
        complexity=DataComplexity.MODERATE,
        size=10,
        seed=42
    )

    # Generate user data
    user_factory = UserTestFactory(config)
    users_df = user_factory.generate_dataframe()
    print(f"Generated {len(users_df)} user records with {len(users_df.columns)} columns")

    # Generate sales data
    sales_factory = SalesTestFactory(config)
    sales_df = sales_factory.generate_dataframe()
    print(f"Generated {len(sales_df)} sales records with {len(sales_df.columns)} columns")

    # Generate API test data
    api_factory = APITestFactory(config)
    api_requests = api_factory.generate_batch(5)
    print(f"Generated {len(api_requests)} API test cases")

    print("\n✓ Comprehensive Test Factories ready for all testing scenarios")
