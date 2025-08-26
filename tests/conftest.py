"""
Pytest Configuration and Fixtures
Provides shared fixtures and configuration for all tests.
"""
import asyncio
import shutil
import tempfile
from collections.abc import AsyncGenerator, Generator
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

try:
    import polars as pl
    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False

try:
    from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
    from sqlalchemy.orm import sessionmaker
    from sqlmodel import SQLModel, create_engine
    SQLMODEL_AVAILABLE = True
except ImportError:
    SQLMODEL_AVAILABLE = False
    AsyncSession = None

try:
    import redis.asyncio as redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False

try:
    import pika
    import aio_pika
    from kafka import KafkaProducer, KafkaConsumer
    MESSAGING_AVAILABLE = True
except ImportError:
    MESSAGING_AVAILABLE = False


# Pytest configuration
def pytest_configure(config):
    """Configure pytest with custom markers."""
    config.addinivalue_line(
        "markers", "slow: mark test as slow running"
    )
    config.addinivalue_line(
        "markers", "integration: mark test as integration test"
    )
    config.addinivalue_line(
        "markers", "external: mark test as requiring external services"
    )
    config.addinivalue_line(
        "markers", "unit: mark test as unit test"
    )
    config.addinivalue_line(
        "markers", "performance: mark test as performance test"
    )
    config.addinivalue_line(
        "markers", "security: mark test as security test"
    )
    config.addinivalue_line(
        "markers", "stress: mark test as stress test"
    )
    config.addinivalue_line(
        "markers", "messaging: mark test as requiring messaging services"
    )
    config.addinivalue_line(
        "markers", "resilience: mark test as resilience test"
    )


def pytest_collection_modifyitems(config, items):
    """Modify test collection to add markers automatically."""
    for item in items:
        # Mark integration tests
        if "integration" in item.nodeid.lower():
            item.add_marker(pytest.mark.integration)

        # Mark slow tests
        if any(keyword in item.name.lower() for keyword in ["performance", "large", "slow"]):
            item.add_marker(pytest.mark.slow)


# Async test support
@pytest.fixture(scope="session")
def event_loop():
    """Create event loop for async tests."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


# Temporary directories and files
@pytest.fixture
def temp_dir() -> Generator[Path, None, None]:
    """Create temporary directory for tests."""
    temp_path = Path(tempfile.mkdtemp())
    yield temp_path
    shutil.rmtree(temp_path)


@pytest.fixture
def temp_file(temp_dir) -> Generator[Path, None, None]:
    """Create temporary file for tests."""
    temp_file_path = temp_dir / "test_file.txt"
    temp_file_path.write_text("test content")
    yield temp_file_path


# Test data fixtures
@pytest.fixture
def sample_sales_data() -> pd.DataFrame:
    """Generate sample sales data for testing."""
    np.random.seed(42)  # For reproducible tests

    n_records = 1000

    # Generate synthetic sales data
    data = {
        "invoice_id": [f"INV-{str(i).zfill(6)}" for i in range(1, n_records + 1)],
        "customer_id": np.random.choice(range(1, 101), n_records),  # 100 unique customers
        "product_id": np.random.choice(range(1, 51), n_records),    # 50 unique products
        "country": np.random.choice(["USA", "UK", "Germany", "France", "Canada"], n_records),
        "quantity": np.random.randint(1, 20, n_records),
        "unit_price": np.round(np.random.uniform(10.0, 1000.0, n_records), 2),
        "discount": np.round(np.random.uniform(0.0, 0.3, n_records), 2),
        "invoice_date": pd.date_range("2023-01-01", periods=n_records, freq="H"),
        "customer_name": [f"Customer {i}" for i in np.random.choice(range(1, 101), n_records)],
        "product_name": [f"Product {i}" for i in np.random.choice(range(1, 51), n_records)]
    }

    df = pd.DataFrame(data)

    # Calculate derived fields
    df["line_total"] = df["quantity"] * df["unit_price"]
    df["discount_amount"] = df["line_total"] * df["discount"]
    df["net_amount"] = df["line_total"] - df["discount_amount"]

    return df


@pytest.fixture
def sample_dirty_data() -> pd.DataFrame:
    """Generate sample dirty data for cleaning tests."""
    return pd.DataFrame({
        "id": [1, 2, 3, 4, 5, 5, None],  # Duplicate and null
        "name": ["John Doe", "  Jane Smith  ", "", None, "Bob Wilson", "Bob Wilson", "Alice"],
        "email": ["john@email.com", "jane@email", "invalid-email", "", "bob@email.com", "bob@email.com", "alice@email.com"],
        "age": [30, 25, -5, 200, 35, 35, None],  # Outliers and null
        "salary": [50000.0, None, 75000.0, 0.0, 60000.0, 60000.0, 55000.0],
        "phone": ["123-456-7890", "555.123.4567", "invalid", "", "555-987-6543", "555-987-6543", "555-111-2222"],
        "created_date": ["2023-01-01", "invalid_date", "2023-01-03", "2023-01-04", "2023-01-05", "2023-01-05", "2023-01-07"]
    })


@pytest.fixture
def sample_scd2_current_data() -> pd.DataFrame:
    """Generate sample SCD2 current data."""
    return pd.DataFrame({
        "customer_id": [1, 2, 3],
        "customer_name": ["John Doe", "Jane Smith", "Bob Wilson"],
        "email": ["john@email.com", "jane@email.com", "bob@email.com"],
        "city": ["New York", "Boston", "Chicago"],
        "valid_from": [
            datetime(2023, 1, 1),
            datetime(2023, 1, 1),
            datetime(2023, 1, 1)
        ],
        "valid_to": [None, None, None],
        "is_current": [True, True, True],
        "version": [1, 1, 1]
    })


@pytest.fixture
def sample_scd2_incoming_data() -> pd.DataFrame:
    """Generate sample SCD2 incoming data with changes."""
    return pd.DataFrame({
        "customer_id": [1, 2, 3, 4],
        "customer_name": ["John Doe", "Jane Smith Updated", "Bob Wilson", "Alice Johnson"],
        "email": ["john.doe@newemail.com", "jane.smith@email.com", "bob@email.com", "alice@email.com"],
        "city": ["New York", "Boston", "Denver", "Seattle"]
    })


# Database fixtures
@pytest.fixture
def sqlite_memory_url() -> str:
    """SQLite in-memory database URL."""
    return "sqlite:///:memory:"


@pytest.fixture
def async_sqlite_memory_url() -> str:
    """Async SQLite in-memory database URL."""
    return "sqlite+aiosqlite:///:memory:"


@pytest.fixture
def sqlite_file_url(temp_dir) -> str:
    """SQLite file database URL."""
    db_file = temp_dir / "test.db"
    return f"sqlite:///{db_file}"


@pytest.fixture
async def async_db_session(async_sqlite_memory_url) -> AsyncGenerator:
    """Create async database session for testing."""
    if not SQLMODEL_AVAILABLE:
        pytest.skip("SQLModel not available")

    engine = create_async_engine(async_sqlite_memory_url)

    # Create tables
    async with engine.begin() as conn:
        await conn.run_sync(SQLModel.metadata.create_all)

    # Create session
    from sqlalchemy.ext.asyncio import async_sessionmaker
    session_maker = async_sessionmaker(engine, class_=AsyncSession, expire_on_commit=False)

    async with session_maker() as session:
        yield session

    await engine.dispose()


@pytest.fixture
def sync_db_session(sqlite_memory_url):
    """Create sync database session for testing."""
    if not SQLMODEL_AVAILABLE:
        pytest.skip("SQLModel not available")

    engine = create_engine(sqlite_memory_url)
    SQLModel.metadata.create_all(engine)

    session_maker = sessionmaker(bind=engine)
    with session_maker() as session:
        yield session


# Redis fixtures
@pytest.fixture
async def redis_client():
    """Create Redis client for testing."""
    if not REDIS_AVAILABLE:
        pytest.skip("Redis not available")

    # Try to connect to Redis (assumes Redis is running locally for tests)
    try:
        client = redis.from_url("redis://localhost:6379/15")  # Use DB 15 for tests
        await client.ping()

        # Clean up test database
        await client.flushdb()

        yield client

        # Clean up after test
        await client.flushdb()
        await client.close()

    except Exception:
        pytest.skip("Redis server not available")


# Messaging fixtures
@pytest.fixture
async def mock_rabbitmq_manager():
    """Create mock RabbitMQ manager for testing."""
    from unittest.mock import AsyncMock, Mock
    import uuid
    from datetime import datetime
    
    manager = Mock()
    manager.connect = Mock(return_value=True)
    manager.publish_task = Mock(return_value=str(uuid.uuid4()))
    manager.consume_tasks = AsyncMock()
    manager.publish_cache_operation = Mock(return_value=str(uuid.uuid4()))
    manager.get_queue_stats = Mock(return_value={
        "queue_name": "test_queue",
        "message_count": 0,
        "consumer_count": 0,
        "timestamp": datetime.now().isoformat()
    })
    manager.close = Mock()
    manager.active_tasks = {}
    manager.task_results = {}
    manager.cache_operations = {}
    manager.state_storage = {}
    return manager


@pytest.fixture
async def mock_kafka_manager():
    """Create mock Kafka manager for testing."""
    from unittest.mock import AsyncMock, Mock
    from datetime import datetime
    
    manager = Mock()
    manager.create_producer = Mock()
    manager.create_consumer = Mock()
    manager.produce_message = Mock(return_value=True)
    manager.consume_messages = AsyncMock()
    manager.create_topic = Mock(return_value=True)
    manager.list_topics = Mock(return_value=["test-topic"])
    manager.get_metrics = Mock(return_value={
        "kafka_metrics": {
            "messages_produced": 0,
            "messages_consumed": 0,
            "errors": 0,
            "topics_created": 0
        },
        "producer_active": False,
        "active_consumers": 0,
        "bootstrap_servers": ["localhost:9092"],
        "timestamp": datetime.now().isoformat()
    })
    manager.close = Mock()
    manager.flush_producer = Mock()
    return manager


@pytest.fixture
async def mock_messaging_cache():
    """Create mock distributed messaging cache for testing."""
    from unittest.mock import AsyncMock, Mock
    
    cache = Mock()
    cache.get = AsyncMock(return_value=None)
    cache.set = AsyncMock(return_value=True)
    cache.delete = AsyncMock(return_value=True)
    cache.exists = AsyncMock(return_value=False)
    cache.invalidate = AsyncMock(return_value=0)
    cache.clear = AsyncMock(return_value=True)
    cache.increment = AsyncMock(return_value=1)
    cache.expire = AsyncMock(return_value=True)
    cache.get_ttl = AsyncMock(return_value=300)
    cache.cache_aside = AsyncMock(return_value="cached_result")
    cache.get_stats = AsyncMock(return_value={
        'hit_count': 0,
        'miss_count': 0,
        'error_count': 0,
        'hit_ratio': 0.0,
        'total_requests': 0,
        'local_cache_size': 0,
        'messaging_stats': {}
    })
    cache.health_check = AsyncMock(return_value={
        'status': 'healthy',
        'response_time_ms': 5.0,
        'components': {
            'rabbitmq': 'healthy',
            'kafka': 'healthy',
            'local_cache': 'healthy'
        }
    })
    return cache


# In-memory messaging fixtures for CI/CD
@pytest.fixture
async def in_memory_message_queue():
    """In-memory message queue for testing without external dependencies."""
    import asyncio
    from collections import defaultdict, deque
    
    class InMemoryMessageQueue:
        def __init__(self):
            self.queues = defaultdict(deque)
            self.subscribers = defaultdict(list)
            self.stats = defaultdict(lambda: {"published": 0, "consumed": 0})
        
        async def publish(self, topic: str, message: dict, key: str = None) -> str:
            message_id = f"msg_{len(self.queues[topic])}_{hash(str(message))}"
            self.queues[topic].append({
                "id": message_id,
                "message": message,
                "key": key,
                "timestamp": asyncio.get_event_loop().time()
            })
            self.stats[topic]["published"] += 1
            return message_id
        
        async def consume(self, topic: str, timeout: float = 1.0):
            if self.queues[topic]:
                message = self.queues[topic].popleft()
                self.stats[topic]["consumed"] += 1
                return message
            return None
        
        async def subscribe(self, topic: str, callback):
            self.subscribers[topic].append(callback)
        
        def get_stats(self, topic: str):
            return self.stats[topic]
        
        def clear(self, topic: str = None):
            if topic:
                self.queues[topic].clear()
                self.stats[topic] = {"published": 0, "consumed": 0}
            else:
                self.queues.clear()
                self.stats.clear()
    
    return InMemoryMessageQueue()


@pytest.fixture
async def in_memory_cache():
    """In-memory cache for testing without external dependencies."""
    import asyncio
    from datetime import datetime, timedelta
    
    class InMemoryCache:
        def __init__(self):
            self.data = {}
            self.expiry = {}
            self.stats = {"hits": 0, "misses": 0, "sets": 0, "deletes": 0}
        
        def _is_expired(self, key: str) -> bool:
            if key not in self.expiry:
                return False
            return datetime.now() > self.expiry[key]
        
        async def get(self, key: str):
            if key in self.data and not self._is_expired(key):
                self.stats["hits"] += 1
                return self.data[key]
            elif key in self.data:  # expired
                del self.data[key]
                del self.expiry[key]
            
            self.stats["misses"] += 1
            return None
        
        async def set(self, key: str, value, ttl: int = None) -> bool:
            self.data[key] = value
            if ttl:
                self.expiry[key] = datetime.now() + timedelta(seconds=ttl)
            self.stats["sets"] += 1
            return True
        
        async def delete(self, key: str) -> bool:
            if key in self.data:
                del self.data[key]
                if key in self.expiry:
                    del self.expiry[key]
                self.stats["deletes"] += 1
                return True
            return False
        
        async def exists(self, key: str) -> bool:
            return key in self.data and not self._is_expired(key)
        
        async def clear(self):
            self.data.clear()
            self.expiry.clear()
        
        def get_stats(self):
            return self.stats.copy()
    
    return InMemoryCache()


# Mock fixtures
@pytest.fixture
def mock_logger():
    """Create mock logger for testing."""
    from unittest.mock import Mock
    return Mock()


@pytest.fixture
def mock_metrics_collector():
    """Create mock metrics collector."""
    from unittest.mock import Mock
    collector = Mock()
    collector.increment_counter = Mock()
    collector.set_gauge = Mock()
    collector.record_histogram = Mock()
    collector.get_metrics_summary = Mock(return_value={
        "timestamp": datetime.utcnow().isoformat(),
        "counters": {},
        "gauges": {},
        "active_jobs": 0,
        "total_jobs": 0
    })
    return collector


@pytest.fixture
def mock_alert_manager():
    """Create mock alert manager."""
    from unittest.mock import AsyncMock, Mock
    manager = Mock()
    manager.evaluate_rules = AsyncMock()
    manager.get_active_alerts = Mock(return_value=[])
    manager.get_alert_summary = Mock(return_value={
        "total_active_alerts": 0,
        "active_by_severity": {"critical": 0, "error": 0, "warning": 0, "info": 0}
    })
    return manager


# Configuration fixtures
@pytest.fixture
def test_config() -> dict:
    """Test configuration dictionary."""
    return {
        "database": {
            "url": "sqlite:///:memory:",
            "echo": False
        },
        "redis": {
            "url": "redis://localhost:6379/15",
            "key_prefix": "test:"
        },
        "monitoring": {
            "enabled": True,
            "metrics_port": 8001,
            "dashboard_port": 8081
        },
        "etl": {
            "default_engine": "pandas",
            "batch_size": 1000,
            "max_retries": 3
        },
        "logging": {
            "level": "DEBUG",
            "format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        }
    }


# Skip markers for external dependencies
def pytest_runtest_setup(item):
    """Setup function to handle test skipping based on markers."""
    # Skip external tests if services are not available
    if item.get_closest_marker("external"):
        # Could add logic here to check if external services are available
        pass
    
    # Skip messaging tests if messaging libraries are not available
    if item.get_closest_marker("messaging") and not MESSAGING_AVAILABLE:
        pytest.skip("Messaging libraries (pika, kafka-python) not available")

    # Skip slow tests unless explicitly requested
    if item.get_closest_marker("slow") and not item.config.getoption("--runslow", default=False):
        pytest.skip("need --runslow option to run")


def pytest_addoption(parser):
    """Add custom command line options."""
    parser.addoption(
        "--runslow",
        action="store_true",
        default=False,
        help="run slow tests"
    )
    parser.addoption(
        "--runexternal",
        action="store_true",
        default=False,
        help="run tests requiring external services"
    )


# Performance testing utilities
@pytest.fixture
def performance_timer():
    """Utility for measuring test performance."""
    import time

    class Timer:
        def __init__(self):
            self.start_time = None
            self.end_time = None

        def start(self):
            self.start_time = time.perf_counter()
            return self

        def stop(self):
            self.end_time = time.perf_counter()
            return self

        @property
        def elapsed(self):
            if self.start_time and self.end_time:
                return self.end_time - self.start_time
            return None

    return Timer()


# Memory profiling utilities
@pytest.fixture
def memory_profiler():
    """Utility for monitoring memory usage in tests."""
    try:
        import os

        import psutil

        class MemoryProfiler:
            def __init__(self):
                self.process = psutil.Process(os.getpid())
                self.initial_memory = None
                self.peak_memory = None

            def start(self):
                self.initial_memory = self.process.memory_info().rss
                return self

            def checkpoint(self):
                current_memory = self.process.memory_info().rss
                if self.peak_memory is None or current_memory > self.peak_memory:
                    self.peak_memory = current_memory
                return current_memory

            @property
            def memory_increase(self):
                if self.initial_memory and self.peak_memory:
                    return self.peak_memory - self.initial_memory
                return None

            @property
            def memory_increase_mb(self):
                increase = self.memory_increase
                return increase / (1024 * 1024) if increase else None

        return MemoryProfiler()

    except ImportError:
        pytest.skip("psutil not available for memory profiling")


# Data validation utilities
@pytest.fixture
def data_validator():
    """Utility for validating test data quality."""
    class DataValidator:
        @staticmethod
        def validate_dataframe(df: pd.DataFrame, expected_columns: list = None,
                             min_rows: int = 0, max_nulls_pct: float = 1.0):
            """Validate DataFrame meets basic quality criteria."""
            errors = []

            if expected_columns:
                missing_cols = set(expected_columns) - set(df.columns)
                if missing_cols:
                    errors.append(f"Missing columns: {missing_cols}")

            if len(df) < min_rows:
                errors.append(f"Insufficient rows: {len(df)} < {min_rows}")

            if not df.empty:
                null_pct = df.isnull().sum().sum() / (len(df) * len(df.columns))
                if null_pct > max_nulls_pct:
                    errors.append(f"Too many nulls: {null_pct:.2%} > {max_nulls_pct:.2%}")

            return errors

        @staticmethod
        def assert_dataframe_equal(df1: pd.DataFrame, df2: pd.DataFrame,
                                 check_dtype: bool = True, check_index: bool = True):
            """Assert two DataFrames are equal with better error messages."""
            try:
                pd.testing.assert_frame_equal(
                    df1, df2,
                    check_dtype=check_dtype,
                    check_index=check_index
                )
            except AssertionError as e:
                # Add more context to assertion errors
                raise AssertionError(
                    f"DataFrames not equal:\n"
                    f"Shape 1: {df1.shape}, Shape 2: {df2.shape}\n"
                    f"Columns 1: {list(df1.columns)}\n"
                    f"Columns 2: {list(df2.columns)}\n"
                    f"Original error: {str(e)}"
                )

    return DataValidator()


# Clean up fixtures
@pytest.fixture(autouse=True)
def cleanup_temp_files():
    """Automatically clean up temporary files after each test."""
    # Setup
    temp_files_before = set()

    yield

    # Teardown - could add cleanup logic here if needed
    pass


# Missing fixtures for test collection
@pytest.fixture
def polars_engine():
    """Polars engine fixture for DataFrame processing."""
    if not POLARS_AVAILABLE:
        pytest.skip("Polars not available")
    
    class PolarsEngine:
        def __init__(self):
            self.name = "polars"
        
        def read_csv(self, file_path, **kwargs):
            return pl.read_csv(file_path, **kwargs)
        
        def read_parquet(self, file_path, **kwargs):
            return pl.read_parquet(file_path, **kwargs)
        
        def to_pandas(self, df):
            return df.to_pandas()
        
        def filter(self, df, condition):
            return df.filter(condition)
        
        def select(self, df, columns):
            return df.select(columns)
    
    return PolarsEngine()


@pytest.fixture
def star_schema_config():
    """Star schema configuration fixture for dimensional modeling tests."""
    return {
        "fact_table": {
            "name": "fact_sales",
            "columns": [
                "invoice_id",
                "customer_key",
                "product_key",
                "time_key",
                "geography_key",
                "quantity",
                "unit_price",
                "discount_amount",
                "line_total",
                "net_amount"
            ],
            "measures": [
                "quantity",
                "unit_price", 
                "discount_amount",
                "line_total",
                "net_amount"
            ],
            "foreign_keys": [
                "customer_key",
                "product_key", 
                "time_key",
                "geography_key"
            ]
        },
        "dimensions": {
            "customer": {
                "table_name": "dim_customer",
                "natural_key": "customer_id",
                "surrogate_key": "customer_key",
                "attributes": [
                    "customer_id",
                    "customer_name",
                    "email",
                    "phone",
                    "registration_date",
                    "customer_segment",
                    "is_active"
                ],
                "scd_type": 2
            },
            "product": {
                "table_name": "dim_product",
                "natural_key": "product_id",
                "surrogate_key": "product_key",
                "attributes": [
                    "product_id",
                    "product_name",
                    "category",
                    "subcategory",
                    "brand",
                    "unit_cost",
                    "is_active"
                ],
                "scd_type": 1
            },
            "time": {
                "table_name": "dim_time",
                "natural_key": "date",
                "surrogate_key": "time_key",
                "attributes": [
                    "date",
                    "year",
                    "quarter",
                    "month",
                    "month_name",
                    "day_of_month",
                    "day_of_week",
                    "day_name",
                    "week_of_year",
                    "is_weekend",
                    "is_holiday"
                ],
                "scd_type": 1
            },
            "geography": {
                "table_name": "dim_geography",
                "natural_key": "country",
                "surrogate_key": "geography_key",
                "attributes": [
                    "country",
                    "country_code",
                    "region",
                    "continent",
                    "population",
                    "gdp_per_capita"
                ],
                "scd_type": 1
            }
        },
        "surrogate_key_strategy": "auto_increment",
        "default_values": {
            "unknown_key": -1,
            "not_applicable_key": -2,
            "missing_key": -3
        }
    }


@pytest.fixture
def sample_silver_data():
    """Sample silver layer data fixture for data pipeline testing."""
    np.random.seed(42)  # For reproducible tests
    
    n_records = 500
    
    # Generate cleaned and standardized data
    data = {
        "invoice_id": [f"INV-{str(i).zfill(6)}" for i in range(1, n_records + 1)],
        "customer_key": np.random.randint(1, 101, n_records),
        "product_key": np.random.randint(1, 51, n_records),
        "time_key": np.random.randint(1, 365, n_records),
        "geography_key": np.random.randint(1, 6, n_records),
        "quantity": np.random.randint(1, 20, n_records),
        "unit_price": np.round(np.random.uniform(10.0, 1000.0, n_records), 2),
        "discount_rate": np.round(np.random.uniform(0.0, 0.3, n_records), 2),
        "line_total": None,  # Will be calculated
        "discount_amount": None,  # Will be calculated  
        "net_amount": None,  # Will be calculated
        "created_at": pd.date_range("2023-01-01", periods=n_records, freq="H"),
        "updated_at": pd.date_range("2023-01-01", periods=n_records, freq="H"),
        "data_quality_score": np.round(np.random.uniform(0.8, 1.0, n_records), 2),
        "source_system": np.random.choice(["ERP", "CRM", "E-commerce"], n_records),
        "batch_id": [f"batch_{(i // 100) + 1}" for i in range(n_records)]
    }
    
    df = pd.DataFrame(data)
    
    # Calculate derived fields
    df["line_total"] = df["quantity"] * df["unit_price"]
    df["discount_amount"] = df["line_total"] * df["discount_rate"]  
    df["net_amount"] = df["line_total"] - df["discount_amount"]
    
    # Add data lineage information
    df["bronze_record_id"] = [f"bronze_{i}" for i in range(1, n_records + 1)]
    df["transformation_version"] = "v1.0"
    df["validation_status"] = np.random.choice(["PASSED", "WARNING"], n_records, p=[0.9, 0.1])
    
    return df


# Test configuration for messaging
@pytest.fixture
def messaging_test_config():
    """Test configuration for messaging components."""
    return {
        "rabbitmq": {
            "host": "localhost",
            "port": 5672,
            "username": "guest",
            "password": "guest",
            "virtual_host": "/",
            "test_queue_prefix": "test_",
            "connection_timeout": 10
        },
        "kafka": {
            "bootstrap_servers": ["localhost:9092"],
            "test_topic_prefix": "test-",
            "consumer_group_prefix": "test-group-",
            "producer_config": {
                "acks": "all",
                "retries": 1,
                "batch_size": 1024
            },
            "consumer_config": {
                "auto_offset_reset": "earliest",
                "enable_auto_commit": True
            }
        },
        "cache": {
            "key_prefix": "test:",
            "default_ttl": 300,
            "max_local_cache_size": 100
        },
        "performance": {
            "max_latency_ms": 1000,
            "min_throughput_msg_per_sec": 100,
            "test_duration_seconds": 30
        }
    }


# Performance testing utilities for messaging
@pytest.fixture
def messaging_performance_tracker():
    """Utility for measuring messaging performance."""
    import time
    from collections import defaultdict
    
    class MessagingPerformanceTracker:
        def __init__(self):
            self.metrics = defaultdict(list)
            self.start_times = {}
        
        def start_operation(self, operation_name: str):
            self.start_times[operation_name] = time.perf_counter()
        
        def end_operation(self, operation_name: str):
            if operation_name in self.start_times:
                duration = time.perf_counter() - self.start_times[operation_name]
                self.metrics[operation_name].append(duration)
                del self.start_times[operation_name]
                return duration
            return None
        
        def get_stats(self, operation_name: str):
            measurements = self.metrics[operation_name]
            if not measurements:
                return None
            
            return {
                "count": len(measurements),
                "min": min(measurements),
                "max": max(measurements),
                "avg": sum(measurements) / len(measurements),
                "total": sum(measurements)
            }
        
        def get_all_stats(self):
            return {op: self.get_stats(op) for op in self.metrics.keys()}
    
    return MessagingPerformanceTracker()


# Message generation utilities
@pytest.fixture
def message_generator():
    """Generate test messages for messaging tests."""
    import uuid
    import random
    from datetime import datetime
    
    class MessageGenerator:
        def __init__(self):
            self.message_counter = 0
        
        def generate_task_message(self, task_name: str = None, payload: dict = None):
            self.message_counter += 1
            return {
                "task_id": str(uuid.uuid4()),
                "task_name": task_name or f"test_task_{self.message_counter}",
                "payload": payload or {"data": f"test_data_{self.message_counter}"},
                "priority": random.choice([1, 5, 8, 10]),
                "created_at": datetime.now().isoformat(),
                "retry_count": 0,
                "max_retries": 3
            }
        
        def generate_cache_message(self, operation: str = "set", key: str = None, value = None):
            self.message_counter += 1
            return {
                "operation_id": str(uuid.uuid4()),
                "operation": operation,
                "key": key or f"test_key_{self.message_counter}",
                "value": value or f"test_value_{self.message_counter}",
                "ttl": 300,
                "created_at": datetime.now().isoformat()
            }
        
        def generate_streaming_message(self, topic: str = None, payload: dict = None):
            self.message_counter += 1
            return {
                "message_id": str(uuid.uuid4()),
                "topic": topic or f"test-topic",
                "key": f"test_key_{self.message_counter}",
                "payload": payload or {"event": f"test_event_{self.message_counter}"},
                "timestamp": datetime.now().isoformat(),
                "headers": {"source": "test", "version": "1.0"}
            }
        
        def generate_batch_messages(self, count: int, message_type: str = "task", **kwargs):
            messages = []
            for _ in range(count):
                if message_type == "task":
                    messages.append(self.generate_task_message(**kwargs))
                elif message_type == "cache":
                    messages.append(self.generate_cache_message(**kwargs))
                elif message_type == "streaming":
                    messages.append(self.generate_streaming_message(**kwargs))
                else:
                    raise ValueError(f"Unknown message type: {message_type}")
            return messages
    
    return MessageGenerator()
