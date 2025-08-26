"""
Performance test data fixtures.
Creates test data optimized for performance and load testing scenarios.
"""
import asyncio
import random
import string
import tempfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd


class PerformanceDataGenerator:
    """Generate test data optimized for performance testing"""

    def __init__(self):
        self.temp_dir = None
        self.data_files = {}

    async def setup(self):
        """Setup temporary directory and generate test data"""
        self.temp_dir = Path(tempfile.mkdtemp(prefix="perf_test_"))
        await self._generate_sales_data()
        await self._generate_product_data()
        await self._generate_customer_data()
        return self

    async def _generate_sales_data(self, num_records: int = 10000):
        """Generate sales transaction data"""
        await asyncio.sleep(0.1)  # Simulate data generation time

        # Generate realistic sales data
        start_date = datetime(2024, 1, 1)
        countries = ['United Kingdom', 'Germany', 'France', 'Italy', 'Spain', 'Netherlands', 'Belgium']
        products = [f'PROD-{i:04d}' for i in range(1, 501)]  # 500 products
        customers = [f'CUST-{i:06d}' for i in range(1, 5001)]  # 5000 customers

        sales_data = []
        for i in range(num_records):
            sales_data.append({
                'InvoiceNo': f'INV-{i:08d}',
                'StockCode': random.choice(products),
                'Description': f'Test Product {random.randint(1, 100)}',
                'Quantity': random.randint(1, 50),
                'InvoiceDate': (start_date + timedelta(days=random.randint(0, 365))).isoformat(),
                'UnitPrice': round(random.uniform(1.0, 500.0), 2),
                'CustomerID': random.choice(customers),
                'Country': random.choice(countries)
            })

        # Save to CSV and Parquet for different test scenarios
        df = pd.DataFrame(sales_data)

        csv_path = self.temp_dir / "sales_data.csv"
        parquet_path = self.temp_dir / "sales_data.parquet"

        df.to_csv(csv_path, index=False)
        df.to_parquet(parquet_path, index=False)

        self.data_files['sales_csv'] = csv_path
        self.data_files['sales_parquet'] = parquet_path

        return df

    async def _generate_product_data(self, num_products: int = 1000):
        """Generate product catalog data"""
        await asyncio.sleep(0.05)

        categories = ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books', 'Toys', 'Health']
        brands = ['Brand-A', 'Brand-B', 'Brand-C', 'Brand-D', 'Brand-E']

        product_data = []
        for i in range(1, num_products + 1):
            product_data.append({
                'ProductID': f'PROD-{i:04d}',
                'ProductName': f'Test Product {i}',
                'Category': random.choice(categories),
                'Brand': random.choice(brands),
                'Price': round(random.uniform(5.0, 1000.0), 2),
                'Stock': random.randint(0, 1000),
                'Rating': round(random.uniform(1.0, 5.0), 1),
                'ReviewCount': random.randint(0, 500),
                'Description': f'Description for product {i} - ' + ''.join(random.choices(string.ascii_lowercase, k=50))
            })

        df = pd.DataFrame(product_data)

        csv_path = self.temp_dir / "products.csv"
        df.to_csv(csv_path, index=False)

        self.data_files['products_csv'] = csv_path
        return df

    async def _generate_customer_data(self, num_customers: int = 5000):
        """Generate customer data"""
        await asyncio.sleep(0.05)

        countries = ['United Kingdom', 'Germany', 'France', 'Italy', 'Spain']
        cities = {
            'United Kingdom': ['London', 'Manchester', 'Birmingham'],
            'Germany': ['Berlin', 'Munich', 'Hamburg'],
            'France': ['Paris', 'Lyon', 'Marseille'],
            'Italy': ['Rome', 'Milan', 'Naples'],
            'Spain': ['Madrid', 'Barcelona', 'Valencia']
        }

        customer_data = []
        for i in range(1, num_customers + 1):
            country = random.choice(countries)
            customer_data.append({
                'CustomerID': f'CUST-{i:06d}',
                'CustomerName': f'Customer {i}',
                'Email': f'customer{i}@test.com',
                'Country': country,
                'City': random.choice(cities[country]),
                'RegistrationDate': (datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1460))).isoformat(),
                'TotalOrders': random.randint(1, 100),
                'TotalSpent': round(random.uniform(50.0, 10000.0), 2),
                'CustomerSegment': random.choice(['Bronze', 'Silver', 'Gold', 'Platinum'])
            })

        df = pd.DataFrame(customer_data)

        csv_path = self.temp_dir / "customers.csv"
        df.to_csv(csv_path, index=False)

        self.data_files['customers_csv'] = csv_path
        return df

    async def generate_large_dataset(self, num_records: int = 100000) -> Path:
        """Generate a large dataset for stress testing"""
        await asyncio.sleep(0.2)

        # Generate large sales dataset
        large_data = []
        batch_size = 10000

        for batch in range(0, num_records, batch_size):
            batch_data = []
            for i in range(batch, min(batch + batch_size, num_records)):
                batch_data.append({
                    'ID': i,
                    'TransactionID': f'TXN-{i:08d}',
                    'Timestamp': (datetime.now() - timedelta(seconds=random.randint(0, 86400*365))).isoformat(),
                    'Amount': round(random.uniform(1.0, 1000.0), 2),
                    'CustomerID': f'CUST-{random.randint(1, 10000):06d}',
                    'ProductID': f'PROD-{random.randint(1, 1000):04d}',
                    'Category': random.choice(['A', 'B', 'C', 'D', 'E']),
                    'Status': random.choice(['completed', 'pending', 'cancelled']),
                    'PaymentMethod': random.choice(['credit_card', 'debit_card', 'paypal', 'bank_transfer']),
                    'Country': random.choice(['UK', 'US', 'DE', 'FR', 'IT']),
                    'ProcessingTime': random.uniform(0.1, 5.0)
                })
            large_data.extend(batch_data)

        df = pd.DataFrame(large_data)
        large_file_path = self.temp_dir / f"large_dataset_{num_records}.parquet"
        df.to_parquet(large_file_path, index=False)

        self.data_files[f'large_dataset_{num_records}'] = large_file_path
        return large_file_path

    def get_data_file(self, file_key: str) -> Path:
        """Get path to a generated data file"""
        return self.data_files.get(file_key)

    def get_all_files(self) -> dict[str, Path]:
        """Get all generated data files"""
        return self.data_files.copy()

    def get_temp_directory(self) -> Path:
        """Get the temporary directory path"""
        return self.temp_dir

    def cleanup(self):
        """Cleanup temporary files and directories"""
        if self.temp_dir and self.temp_dir.exists():
            import shutil
            shutil.rmtree(self.temp_dir)
        self.data_files.clear()


# Global performance data generator instance
_performance_data_generator = None


async def create_performance_test_data() -> PerformanceDataGenerator:
    """
    Create and setup performance test data.
    
    Returns:
        PerformanceDataGenerator instance with test data ready
    """
    global _performance_data_generator

    if _performance_data_generator is None:
        _performance_data_generator = PerformanceDataGenerator()
        await _performance_data_generator.setup()

    return _performance_data_generator


async def get_sample_sales_data(num_records: int = 1000) -> list[dict[str, Any]]:
    """Get sample sales data for API testing"""
    countries = ['United Kingdom', 'Germany', 'France']
    products = ['Heart Pendant', 'Coffee Mug', 'Notebook', 'Pen Set', 'Calendar']

    sales_data = []
    for i in range(num_records):
        sales_data.append({
            'InvoiceNo': f'INV-{i:06d}',
            'StockCode': f'ITEM-{random.randint(1, 100):03d}',
            'Description': random.choice(products),
            'Quantity': random.randint(1, 20),
            'InvoiceDate': (datetime.now() - timedelta(days=random.randint(0, 365))).isoformat(),
            'UnitPrice': round(random.uniform(1.0, 100.0), 2),
            'CustomerID': f'CUST-{random.randint(1, 1000):04d}',
            'Country': random.choice(countries)
        })

    return sales_data


async def create_stress_test_dataset(size_mb: int = 100) -> Path:
    """
    Create a dataset of specific size for stress testing.
    
    Args:
        size_mb: Target size in megabytes
    
    Returns:
        Path to the created dataset file
    """
    # Estimate records needed for target size (approximate)
    estimated_record_size = 200  # bytes per record
    target_records = (size_mb * 1024 * 1024) // estimated_record_size

    generator = PerformanceDataGenerator()
    await generator.setup()

    return await generator.generate_large_dataset(target_records)


def get_test_query_patterns() -> list[dict[str, Any]]:
    """Get common query patterns for performance testing"""
    return [
        {
            'name': 'simple_filter',
            'params': {'country': 'United Kingdom', 'limit': 100}
        },
        {
            'name': 'date_range',
            'params': {'start_date': '2024-01-01', 'end_date': '2024-01-31', 'limit': 100}
        },
        {
            'name': 'aggregation',
            'params': {'group_by': 'country', 'metric': 'total_sales', 'limit': 10}
        },
        {
            'name': 'complex_filter',
            'params': {'country': 'United Kingdom', 'min_amount': 10, 'max_amount': 1000, 'limit': 50}
        },
        {
            'name': 'search_query',
            'params': {'q': 'heart', 'type': 'products', 'limit': 20}
        },
        {
            'name': 'pagination_test',
            'params': {'page': 5, 'size': 100}
        }
    ]


async def create_concurrent_test_data(num_datasets: int = 5) -> list[Path]:
    """Create multiple datasets for concurrent testing"""
    generators = []
    for i in range(num_datasets):
        generator = PerformanceDataGenerator()
        await generator.setup()
        generators.append(generator)

    return [gen.get_temp_directory() for gen in generators]
