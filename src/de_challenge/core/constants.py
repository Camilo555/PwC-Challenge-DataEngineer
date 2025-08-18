"""
Application constants and enumerations.
Central location for all magic numbers and constant values.
"""

from enum import Enum
from typing import Final


class DataLayers(str, Enum):
    """Medallion architecture data layers."""

    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"


class FileTypes(str, Enum):
    """Supported file types for ingestion."""

    CSV = "csv"
    JSON = "json"
    PARQUET = "parquet"
    EXCEL = "xlsx"
    PDF = "pdf"


class TableNames(str, Enum):
    """Database table names for star schema."""

    # Fact tables
    FACT_SALE = "fact_sale"

    # Dimension tables
    DIM_PRODUCT = "dim_product"
    DIM_CUSTOMER = "dim_customer"
    DIM_DATE = "dim_date"
    DIM_COUNTRY = "dim_country"
    DIM_INVOICE = "dim_invoice"
    DIM_TIME = "dim_time"  # Optional 6th dimension


# ETL Processing Constants
DEFAULT_BATCH_SIZE: Final[int] = 10000
DEFAULT_PARALLELISM: Final[int] = 4
MAX_RETRIES: Final[int] = 3
RETRY_DELAY_SECONDS: Final[int] = 5

# Data Quality Constants
MIN_VALID_QUANTITY: Final[int] = -100000
MAX_VALID_QUANTITY: Final[int] = 100000
MIN_VALID_PRICE: Final[float] = 0.0
MAX_VALID_PRICE: Final[float] = 1000000.0

# Stock Code Patterns
STOCK_CODE_PATTERN: Final[str] = r"^[A-Z0-9]{3,10}$"
CANCELLED_INVOICE_PREFIX: Final[str] = "C"

# Date Formats
INPUT_DATE_FORMAT: Final[str] = "%Y-%m-%d %H:%M:%S"
OUTPUT_DATE_FORMAT: Final[str] = "%Y-%m-%d"

# Vector Search Constants
EMBEDDING_MODEL: Final[str] = "sentence-transformers/all-MiniLM-L6-v2"
EMBEDDING_DIMENSION: Final[int] = 384
VECTOR_SEARCH_LIMIT: Final[int] = 100
MIN_SEARCH_SCORE: Final[float] = 0.5

# API Constants
API_VERSION: Final[str] = "v1"
API_PREFIX: Final[str] = f"/api/{API_VERSION}"
MAX_PAGE_SIZE: Final[int] = 1000
DEFAULT_PAGE_SIZE: Final[int] = 100

# Cache Constants
CACHE_TTL_SECONDS: Final[int] = 3600  # 1 hour
CACHE_KEY_PREFIX: Final[str] = "de_challenge"

# Monitoring Constants
METRICS_PREFIX: Final[str] = "de_challenge"
HEALTH_CHECK_TIMEOUT: Final[int] = 5

# File Size Limits
MAX_FILE_SIZE_MB: Final[int] = 500
MAX_PDF_PAGES: Final[int] = 100

# Spark Optimization Constants
SPARK_BROADCAST_THRESHOLD_MB: Final[int] = 10
SPARK_PARTITION_SIZE_MB: Final[int] = 128

# Delta Lake Constants
DELTA_VACUUM_RETENTION_HOURS: Final[int] = 168  # 7 days
DELTA_OPTIMIZE_THRESHOLD_GB: Final[float] = 1.0

# Database Constants
DB_CONNECTION_POOL_SIZE: Final[int] = 20
DB_CONNECTION_MAX_OVERFLOW: Final[int] = 10
DB_CONNECTION_TIMEOUT_SECONDS: Final[int] = 30

# Typesense Constants
TYPESENSE_COLLECTION_NAME: Final[str] = "products"
TYPESENSE_BATCH_SIZE: Final[int] = 100
TYPESENSE_SEARCH_TIMEOUT_MS: Final[int] = 3000

# Error Messages
ERROR_INVALID_FILE_TYPE: Final[str] = "Invalid file type. Supported types: {types}"
ERROR_FILE_NOT_FOUND: Final[str] = "File not found: {file_path}"
ERROR_INVALID_DATE_FORMAT: Final[str] = "Invalid date format. Expected: {format}"
ERROR_VALIDATION_FAILED: Final[str] = "Validation failed: {details}"
ERROR_ETL_FAILED: Final[str] = "ETL process failed: {details}"
ERROR_DATABASE_CONNECTION: Final[str] = "Database connection failed: {details}"
ERROR_API_AUTHENTICATION: Final[str] = "Authentication failed"
ERROR_RATE_LIMIT: Final[str] = "Rate limit exceeded"

# Success Messages
SUCCESS_ETL_COMPLETE: Final[str] = "ETL process completed successfully"
SUCCESS_DATA_INGESTED: Final[str] = "Data ingested successfully: {count} records"
SUCCESS_INDEX_CREATED: Final[str] = "Search index created successfully"
SUCCESS_API_STARTED: Final[str] = "API started successfully on {host}:{port}"
