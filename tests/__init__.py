"""
Test Package for PwC Data Engineer Challenge
Contains comprehensive unit and integration tests.
"""

# Test configuration
TEST_CONFIG = {
    "database": {
        "test_url": "sqlite:///:memory:",
        "timeout": 30
    },
    "redis": {
        "test_url": "redis://localhost:6379/15",
        "timeout": 5
    },
    "performance": {
        "max_execution_time": 30,
        "max_memory_mb": 500
    }
}

__version__ = "1.0.0"