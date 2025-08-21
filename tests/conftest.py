"""
Pytest Configuration and Fixtures
Provides shared fixtures and configuration for all tests.
"""
import pytest
import asyncio
import tempfile
import shutil
from pathlib import Path
from datetime import datetime
from typing import Generator, AsyncGenerator
import pandas as pd
import numpy as np

try:
    from sqlmodel import SQLModel, create_engine
    from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
    from sqlalchemy.orm import sessionmaker
    SQLMODEL_AVAILABLE = True
except ImportError:
    SQLMODEL_AVAILABLE = False

try:
    import redis.asyncio as redis
    REDIS_AVAILABLE = True
except ImportError:
    REDIS_AVAILABLE = False


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
async def async_db_session(async_sqlite_memory_url) -> AsyncGenerator[AsyncSession, None]:
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
    from unittest.mock import Mock, AsyncMock
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
        import psutil
        import os
        
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