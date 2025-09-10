"""
Comprehensive pytest configuration for BMAD testing framework.

This module provides enterprise-grade testing fixtures and configurations
for all BMAD stories with 95%+ coverage targets.
"""

from __future__ import annotations

import asyncio
import json
import os
import tempfile
from typing import Any, AsyncIterator, Iterator
from unittest.mock import AsyncMock, MagicMock

import pytest
import pytest_asyncio
from faker import Faker
from httpx import AsyncClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from testcontainers.postgres import PostgresContainer

# Configure pytest-asyncio
pytest_asyncio.plugin.Mode.AUTO


@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for the test session."""
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()


@pytest.fixture(scope="session")
def faker_instance() -> Faker:
    """Provide a Faker instance for generating test data."""
    faker = Faker(['en_US'])
    Faker.seed(42)  # Deterministic test data
    return faker


@pytest.fixture(scope="session")
def postgres_container() -> Iterator[PostgresContainer]:
    """Provide a PostgreSQL container for integration tests."""
    with PostgresContainer("postgres:15-alpine") as postgres:
        yield postgres


@pytest.fixture(scope="session")
def database_url(postgres_container: PostgresContainer) -> str:
    """Provide database URL for testing."""
    return postgres_container.get_connection_url()


@pytest.fixture(scope="session") 
def database_engine(database_url: str):
    """Provide SQLAlchemy engine for testing."""
    engine = create_engine(database_url)
    yield engine
    engine.dispose()


@pytest.fixture
def database_session(database_engine):
    """Provide database session with transaction rollback."""
    Session = sessionmaker(bind=database_engine)
    session = Session()
    
    # Begin transaction
    transaction = session.begin()
    
    yield session
    
    # Rollback transaction to ensure test isolation
    transaction.rollback()
    session.close()


@pytest.fixture
async def async_client() -> AsyncIterator[AsyncClient]:
    """Provide async HTTP client for API testing."""
    async with AsyncClient() as client:
        yield client


@pytest.fixture
def mock_redis():
    """Provide mocked Redis client."""
    redis_mock = MagicMock()
    redis_mock.get.return_value = None
    redis_mock.set.return_value = True
    redis_mock.delete.return_value = True
    redis_mock.exists.return_value = False
    return redis_mock


@pytest.fixture
def mock_websocket():
    """Provide mocked WebSocket connection."""
    websocket_mock = AsyncMock()
    websocket_mock.send_text = AsyncMock()
    websocket_mock.receive_text = AsyncMock()
    websocket_mock.accept = AsyncMock()
    websocket_mock.close = AsyncMock()
    return websocket_mock


# Story 1.1: Real-Time BI Dashboard Fixtures
@pytest.fixture
def dashboard_test_data(faker_instance: Faker) -> dict[str, Any]:
    """Generate test data for dashboard tests."""
    return {
        "user_id": faker_instance.uuid4(),
        "dashboard_id": faker_instance.uuid4(),
        "widgets": [
            {
                "id": faker_instance.uuid4(),
                "type": "chart",
                "title": faker_instance.sentence(nb_words=4),
                "data": [faker_instance.random_int(min=1, max=100) for _ in range(10)]
            }
            for _ in range(faker_instance.random_int(min=1, max=5))
        ],
        "refresh_rate": faker_instance.random_int(min=1000, max=60000),
        "permissions": ["read", "write"] if faker_instance.boolean() else ["read"]
    }


@pytest.fixture
def websocket_messages(faker_instance: Faker) -> list[dict[str, Any]]:
    """Generate WebSocket test messages."""
    return [
        {
            "type": "data_update",
            "payload": {
                "widget_id": faker_instance.uuid4(),
                "data": [faker_instance.random_int(min=1, max=100) for _ in range(10)],
                "timestamp": faker_instance.date_time().isoformat()
            }
        }
        for _ in range(faker_instance.random_int(min=5, max=15))
    ]


# Story 1.2: ML Data Quality Framework Fixtures
@pytest.fixture
def ml_test_dataset(faker_instance: Faker) -> dict[str, Any]:
    """Generate ML test dataset."""
    n_samples = faker_instance.random_int(min=100, max=1000)
    return {
        "features": [
            [faker_instance.random_element(elements=[1, 2, 3, 4, 5]) for _ in range(5)]
            for _ in range(n_samples)
        ],
        "labels": [faker_instance.random_int(min=0, max=1) for _ in range(n_samples)],
        "feature_names": [f"feature_{i}" for i in range(5)],
        "metadata": {
            "source": faker_instance.word(),
            "version": faker_instance.random_element(elements=["1.0", "1.1", "2.0"]),
            "created_at": faker_instance.date_time().isoformat()
        }
    }


@pytest.fixture
def data_quality_rules() -> dict[str, Any]:
    """Provide data quality validation rules."""
    return {
        "completeness": {
            "threshold": 0.95,
            "required_fields": ["id", "name", "email", "created_at"]
        },
        "uniqueness": {
            "fields": ["id", "email"],
            "threshold": 1.0
        },
        "validity": {
            "email": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
            "phone": r"^\+?1?\d{9,15}$"
        },
        "consistency": {
            "date_formats": ["ISO 8601"],
            "currency_formats": ["USD", "EUR", "GBP"]
        }
    }


# Story 2.1: Zero-Trust Security Fixtures
@pytest.fixture
def jwt_token(faker_instance: Faker) -> str:
    """Generate JWT token for testing."""
    # This is a mock JWT token for testing - not for production
    import jwt
    payload = {
        "user_id": faker_instance.uuid4(),
        "username": faker_instance.user_name(),
        "email": faker_instance.email(),
        "roles": ["user", "admin"] if faker_instance.boolean() else ["user"],
        "exp": faker_instance.future_datetime().timestamp()
    }
    return jwt.encode(payload, "test-secret", algorithm="HS256")


@pytest.fixture
def security_test_cases() -> list[dict[str, Any]]:
    """Provide security test cases for penetration testing."""
    return [
        {
            "name": "SQL Injection",
            "payload": "'; DROP TABLE users; --",
            "expected_blocked": True
        },
        {
            "name": "XSS Attack",
            "payload": "<script>alert('XSS')</script>",
            "expected_blocked": True
        },
        {
            "name": "Path Traversal",
            "payload": "../../etc/passwd",
            "expected_blocked": True
        },
        {
            "name": "Command Injection",
            "payload": "; cat /etc/passwd",
            "expected_blocked": True
        }
    ]


# Story 2.2: API Performance Fixtures
@pytest.fixture
def performance_test_config() -> dict[str, Any]:
    """Provide performance test configuration."""
    return {
        "api_endpoints": [
            {"path": "/api/v1/users", "method": "GET", "expected_time_ms": 25},
            {"path": "/api/v1/orders", "method": "GET", "expected_time_ms": 50},
            {"path": "/api/v1/products", "method": "GET", "expected_time_ms": 30},
            {"path": "/api/v1/analytics", "method": "POST", "expected_time_ms": 100}
        ],
        "load_test": {
            "concurrent_users": [10, 50, 100, 500, 1000],
            "duration_seconds": 60,
            "ramp_up_seconds": 30
        },
        "sla_requirements": {
            "response_time_p95_ms": 25,
            "error_rate_max_percent": 0.1,
            "throughput_min_rps": 1000
        }
    }


# Story 3.1: Self-Service Analytics Fixtures
@pytest.fixture
def nlp_test_queries(faker_instance: Faker) -> list[dict[str, Any]]:
    """Generate NLP test queries for analytics testing."""
    return [
        {
            "query": "Show me sales data for the last quarter",
            "expected_intent": "sales_query",
            "expected_filters": {"date_range": "last_quarter"},
            "expected_accuracy": 0.95
        },
        {
            "query": "What are the top 10 products by revenue?",
            "expected_intent": "product_ranking",
            "expected_filters": {"metric": "revenue", "limit": 10},
            "expected_accuracy": 0.90
        },
        {
            "query": "Compare customer satisfaction between regions",
            "expected_intent": "comparative_analysis",
            "expected_filters": {"metric": "satisfaction", "group_by": "region"},
            "expected_accuracy": 0.88
        }
    ]


@pytest.fixture
def ui_test_components() -> list[dict[str, Any]]:
    """Provide UI components for testing."""
    return [
        {
            "name": "SearchBar",
            "props": {"placeholder": "Search analytics..."},
            "expected_events": ["onSearch", "onChange"]
        },
        {
            "name": "DataVisualization", 
            "props": {"chart_type": "bar", "data": []},
            "expected_events": ["onDataLoad", "onError"]
        },
        {
            "name": "FilterPanel",
            "props": {"filters": []},
            "expected_events": ["onFilterChange", "onReset"]
        }
    ]


# Test Environment Configuration
@pytest.fixture(scope="session", autouse=True)
def test_environment_setup():
    """Set up test environment variables."""
    os.environ.update({
        "ENVIRONMENT": "test",
        "DATABASE_URL": "postgresql://test:test@localhost:5432/test_db",
        "REDIS_URL": "redis://localhost:6379/1",
        "JWT_SECRET_KEY": "test-jwt-secret-key",
        "API_BASE_URL": "http://localhost:8000",
        "LOG_LEVEL": "DEBUG",
        "ENABLE_METRICS": "false"
    })
    yield
    # Cleanup if needed


@pytest.fixture
def temp_directory() -> Iterator[str]:
    """Provide temporary directory for test files."""
    with tempfile.TemporaryDirectory() as temp_dir:
        yield temp_dir


@pytest.fixture
def mock_external_api():
    """Mock external API responses."""
    class MockExternalAPI:
        def __init__(self):
            self.responses = {}
            
        def set_response(self, endpoint: str, response: dict[str, Any]):
            self.responses[endpoint] = response
            
        async def get(self, endpoint: str) -> dict[str, Any]:
            return self.responses.get(endpoint, {"error": "Not mocked"})
    
    return MockExternalAPI()


# Performance and Load Testing Fixtures
@pytest.fixture
def load_test_scenarios() -> list[dict[str, Any]]:
    """Define load testing scenarios."""
    return [
        {
            "name": "Normal Load",
            "users": 100,
            "spawn_rate": 10,
            "duration": "5m"
        },
        {
            "name": "Peak Load", 
            "users": 1000,
            "spawn_rate": 50,
            "duration": "10m"
        },
        {
            "name": "Stress Test",
            "users": 5000,
            "spawn_rate": 100, 
            "duration": "15m"
        }
    ]


# Data Quality Testing Fixtures
@pytest.fixture
def data_quality_test_cases(faker_instance: Faker) -> dict[str, Any]:
    """Generate comprehensive data quality test cases."""
    return {
        "valid_data": {
            "users": [
                {
                    "id": faker_instance.uuid4(),
                    "name": faker_instance.name(),
                    "email": faker_instance.email(),
                    "created_at": faker_instance.date_time().isoformat(),
                    "age": faker_instance.random_int(min=18, max=80)
                }
                for _ in range(100)
            ]
        },
        "invalid_data": {
            "users": [
                {
                    "id": None,  # Invalid: null ID
                    "name": "",  # Invalid: empty name
                    "email": "invalid-email",  # Invalid: malformed email
                    "created_at": "invalid-date",  # Invalid: malformed date
                    "age": -5  # Invalid: negative age
                }
            ]
        },
        "duplicate_data": {
            "users": [
                {
                    "id": "duplicate-id",
                    "name": "Duplicate User",
                    "email": "duplicate@example.com",
                    "created_at": faker_instance.date_time().isoformat(),
                    "age": 30
                }
            ] * 2  # Duplicate records
        }
    }


# Configuration for different test types
def pytest_configure(config):
    """Configure pytest with custom settings."""
    # Create reports directory if it doesn't exist
    os.makedirs("reports", exist_ok=True)
    
    # Set test markers dynamically
    config.addinivalue_line(
        "markers",
        "bmad_story: Mark test as part of specific BMAD story implementation"
    )


def pytest_collection_modifyitems(config, items):
    """Modify test collection to add automatic markers."""
    for item in items:
        # Add story markers based on test file location
        if "/story_1_1/" in str(item.fspath):
            item.add_marker(pytest.mark.story_1_1)
        elif "/story_1_2/" in str(item.fspath):
            item.add_marker(pytest.mark.story_1_2)
        elif "/story_2_1/" in str(item.fspath):
            item.add_marker(pytest.mark.story_2_1)
        elif "/story_2_2/" in str(item.fspath):
            item.add_marker(pytest.mark.story_2_2)
        elif "/story_3_1/" in str(item.fspath):
            item.add_marker(pytest.mark.story_3_1)
            
        # Add performance marker for slow tests
        if hasattr(item.obj, "__name__") and "performance" in item.obj.__name__:
            item.add_marker(pytest.mark.performance)
            item.add_marker(pytest.mark.slow)


# Cleanup fixtures
@pytest.fixture(autouse=True)
def cleanup_test_data():
    """Automatically cleanup test data after each test."""
    yield
    # Perform cleanup operations here if needed
    # For example: clear temporary files, reset mocks, etc.
    pass