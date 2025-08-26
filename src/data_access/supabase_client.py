"""
Supabase integration module for the PwC Data Engineering Challenge.
Provides secure connection management, schema operations, and data integrity checks.
"""
from __future__ import annotations

import logging
import time
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import pandas as pd
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker
from sqlmodel import SQLModel

from core.config import settings
from core.exceptions import DatabaseConnectionError

logger = logging.getLogger(__name__)


class SupabaseClient:
    """
    Advanced Supabase client with comprehensive database operations.
    Handles connection pooling, schema management, and data integrity.
    """

    def __init__(self) -> None:
        """Initialize Supabase client with configuration validation."""
        self._engine: Engine | None = None
        self._session_factory: sessionmaker | None = None
        self._validate_configuration()

    def _validate_configuration(self) -> None:
        """Validate Supabase configuration settings."""
        if not settings.database_url:
            raise DatabaseConnectionError("DATABASE_URL is required for Supabase connection")

        if settings.database_type.value != "postgresql":
            logger.warning(f"Database type is {settings.database_type}, but using Supabase configuration")

    @property
    def engine(self) -> Engine:
        """Get or create SQLAlchemy engine for Supabase."""
        if self._engine is None:
            connection_args = {}

            # Add SSL mode for Supabase connections
            if "supabase.co" in settings.database_url:
                connection_args["sslmode"] = "require"

            self._engine = create_engine(
                settings.database_url,
                pool_size=10,
                max_overflow=20,
                pool_pre_ping=True,
                pool_recycle=3600,  # Recycle connections every hour
                connect_args=connection_args,
                echo=(settings.environment.value == "development")
            )
            logger.info("Supabase engine created successfully")

        return self._engine

    @property
    def session_factory(self) -> sessionmaker:
        """Get or create session factory."""
        if self._session_factory is None:
            self._session_factory = sessionmaker(bind=self.engine, expire_on_commit=False)
        return self._session_factory

    async def test_connection(self, max_retries: int = 3) -> dict[str, Any]:
        """
        Test Supabase connection with retry logic.

        Args:
            max_retries: Maximum number of retry attempts

        Returns:
            Dict containing connection status and database info
        """
        for attempt in range(max_retries + 1):
            try:
                with self.engine.connect() as conn:
                    # Test basic connectivity
                    result = conn.execute(text("SELECT version(), current_database(), current_user"))
                    row = result.fetchone()

                    if row:
                        version, database, user = row
                        logger.info(f"Connected to Supabase database '{database}' as user '{user}' (attempt {attempt + 1})")

                        return {
                            "status": "connected",
                            "database": database,
                            "user": user,
                            "version": version,
                            "ssl_enabled": "supabase.co" in settings.database_url,
                            "connection_attempt": attempt + 1,
                            "retry_count": attempt
                        }

            except Exception as e:
                if attempt < max_retries:
                    retry_delay = 2 ** attempt  # Exponential backoff
                    logger.warning(f"Connection attempt {attempt + 1} failed: {e}. Retrying in {retry_delay}s...")
                    time.sleep(retry_delay)
                else:
                    logger.error(f"All connection attempts failed. Last error: {e}")
                    raise DatabaseConnectionError(f"Failed to connect to Supabase after {max_retries + 1} attempts: {e}") from e

        raise DatabaseConnectionError("Unknown connection error")

    async def create_schema_if_not_exists(self, schema_name: str = "retail_dwh") -> bool:
        """
        Create database schema if it doesn't exist.

        Args:
            schema_name: Name of the schema to create

        Returns:
            True if schema was created or already exists
        """
        try:
            with self.engine.connect() as conn:
                # Check if schema exists
                result = conn.execute(text("""
                    SELECT schema_name
                    FROM information_schema.schemata
                    WHERE schema_name = :schema_name
                """), {"schema_name": schema_name})

                if not result.fetchone():
                    # Create schema
                    conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema_name}"))
                    conn.commit()
                    logger.info(f"Created schema '{schema_name}'")
                    return True
                else:
                    logger.info(f"Schema '{schema_name}' already exists")
                    return True

        except Exception as e:
            logger.error(f"Failed to create schema '{schema_name}': {e}")
            raise DatabaseConnectionError(f"Schema creation failed: {e}") from e

    async def create_tables(self) -> None:
        """Create all star schema tables in Supabase."""
        try:
            # Create all tables using SQLModel
            SQLModel.metadata.create_all(self.engine)
            logger.info("All star schema tables created successfully")

            # Add indexes for better performance
            await self._create_performance_indexes()

        except Exception as e:
            logger.error(f"Failed to create tables: {e}")
            raise DatabaseConnectionError(f"Table creation failed: {e}") from e

    async def _create_performance_indexes(self) -> None:
        """Create performance indexes for the star schema."""
        indexes = [
            # Fact table indexes
            "CREATE INDEX IF NOT EXISTS idx_fact_sale_product_key ON fact_sale(product_key)",
            "CREATE INDEX IF NOT EXISTS idx_fact_sale_customer_key ON fact_sale(customer_key)",
            "CREATE INDEX IF NOT EXISTS idx_fact_sale_date_key ON fact_sale(date_key)",
            "CREATE INDEX IF NOT EXISTS idx_fact_sale_country_key ON fact_sale(country_key)",
            "CREATE INDEX IF NOT EXISTS idx_fact_sale_invoice_key ON fact_sale(invoice_key)",

            # Dimension table indexes
            "CREATE INDEX IF NOT EXISTS idx_dim_product_stock_code ON dim_product(stock_code)",
            "CREATE INDEX IF NOT EXISTS idx_dim_customer_customer_id ON dim_customer(customer_id)",
            "CREATE INDEX IF NOT EXISTS idx_dim_country_country_name ON dim_country(country_name)",
            "CREATE INDEX IF NOT EXISTS idx_dim_invoice_invoice_no ON dim_invoice(invoice_no)",
            "CREATE INDEX IF NOT EXISTS idx_dim_date_date ON dim_date(date)",
        ]

        try:
            with self.engine.connect() as conn:
                for index_sql in indexes:
                    conn.execute(text(index_sql))
                conn.commit()
                logger.info("Performance indexes created successfully")

        except Exception as e:
            logger.warning(f"Some indexes might not have been created: {e}")

    async def validate_data_integrity(self) -> dict[str, Any]:
        """
        Perform comprehensive data integrity checks on the star schema.

        Returns:
            Dict containing integrity check results
        """
        integrity_report: dict[str, Any] = {
            "status": "passed",
            "checks": [],
            "errors": [],
            "warnings": []
        }

        try:
            with self.session_factory() as session:
                # Check 1: Referential integrity between fact and dimensions
                await self._check_referential_integrity(session, integrity_report)

                # Check 2: Data quality checks
                await self._check_data_quality(session, integrity_report)

                # Check 3: Business rule validations
                await self._check_business_rules(session, integrity_report)

        except Exception as e:
            integrity_report["status"] = "failed"
            integrity_report["errors"].append(f"Integrity validation failed: {e}")
            logger.error(f"Data integrity validation error: {e}")

        return integrity_report

    async def _check_referential_integrity(self, session, report: dict[str, Any]) -> None:
        """Check referential integrity constraints."""
        checks = [
            ("Orphaned fact records (missing product)", """
                SELECT COUNT(*) FROM fact_sale f
                LEFT JOIN dim_product p ON f.product_key = p.product_key
                WHERE p.product_key IS NULL
            """),
            ("Orphaned fact records (missing customer)", """
                SELECT COUNT(*) FROM fact_sale f
                LEFT JOIN dim_customer c ON f.customer_key = c.customer_key
                WHERE f.customer_key IS NOT NULL AND c.customer_key IS NULL
            """),
            ("Orphaned fact records (missing country)", """
                SELECT COUNT(*) FROM fact_sale f
                LEFT JOIN dim_country co ON f.country_key = co.country_key
                WHERE co.country_key IS NULL
            """),
            ("Orphaned fact records (missing invoice)", """
                SELECT COUNT(*) FROM fact_sale f
                LEFT JOIN dim_invoice i ON f.invoice_key = i.invoice_key
                WHERE i.invoice_key IS NULL
            """),
        ]

        for check_name, query in checks:
            try:
                result = session.execute(text(query)).scalar()
                if result and result > 0:
                    report["errors"].append(f"{check_name}: {result} orphaned records found")
                    report["status"] = "failed"
                else:
                    report["checks"].append(f"{check_name}: PASSED")
            except Exception as e:
                report["warnings"].append(f"{check_name}: Could not execute - {e}")

    async def _check_data_quality(self, session, report: dict[str, Any]) -> None:
        """Check data quality constraints."""
        checks = [
            ("Negative quantities in facts", """
                SELECT COUNT(*) FROM fact_sale WHERE quantity < 0
            """),
            ("Zero or negative prices", """
                SELECT COUNT(*) FROM fact_sale WHERE unit_price <= 0
            """),
            ("Missing product descriptions", """
                SELECT COUNT(*) FROM dim_product WHERE description IS NULL OR description = ''
            """),
            ("Invalid total amounts", """
                SELECT COUNT(*) FROM fact_sale
                WHERE ABS(total_amount - (quantity * unit_price)) > 0.01
            """),
        ]

        for check_name, query in checks:
            try:
                result = session.execute(text(query)).scalar()
                if result and result > 0:
                    report["warnings"].append(f"{check_name}: {result} records with issues")
                else:
                    report["checks"].append(f"{check_name}: PASSED")
            except Exception as e:
                report["warnings"].append(f"{check_name}: Could not execute - {e}")

    async def _check_business_rules(self, session, report: dict[str, Any]) -> None:
        """Check business rule compliance."""
        try:
            # Check for duplicate dimension entries
            duplicate_checks = [
                ("Duplicate products by stock_code", """
                    SELECT stock_code, COUNT(*) as cnt
                    FROM dim_product
                    GROUP BY stock_code
                    HAVING COUNT(*) > 1
                """),
                ("Duplicate customers by customer_id", """
                    SELECT customer_id, COUNT(*) as cnt
                    FROM dim_customer
                    WHERE customer_id IS NOT NULL
                    GROUP BY customer_id
                    HAVING COUNT(*) > 1
                """),
            ]

            for check_name, query in duplicate_checks:
                result = session.execute(text(query)).fetchall()
                if result:
                    report["warnings"].append(f"{check_name}: {len(result)} duplicates found")
                else:
                    report["checks"].append(f"{check_name}: PASSED")

        except Exception as e:
            report["warnings"].append(f"Business rules check failed: {e}")

    async def get_table_statistics(self) -> dict[str, dict[str, Any]]:
        """
        Get comprehensive statistics for all tables.

        Returns:
            Dict containing statistics for each table
        """
        tables = [
            "fact_sale",
            "dim_product",
            "dim_customer",
            "dim_country",
            "dim_invoice",
            "dim_date"
        ]

        statistics = {}

        try:
            with self.engine.connect() as conn:
                for table in tables:
                    try:
                        # Get row count
                        count_result = conn.execute(text(f"SELECT COUNT(*) FROM {table}")).scalar()

                        # Get table size (PostgreSQL specific)
                        size_query = text("""
                            SELECT pg_size_pretty(pg_total_relation_size(:table_name)) as size,
                                   pg_size_pretty(pg_relation_size(:table_name)) as table_size
                        """)
                        size_result = conn.execute(size_query, {"table_name": table}).fetchone()

                        statistics[table] = {
                            "row_count": count_result or 0,
                            "total_size": size_result[0] if size_result else "Unknown",
                            "table_size": size_result[1] if size_result else "Unknown"
                        }

                    except Exception as e:
                        statistics[table] = {
                            "row_count": 0,
                            "error": str(e)
                        }

        except Exception as e:
            logger.error(f"Failed to get table statistics: {e}")
            raise DatabaseConnectionError(f"Statistics query failed: {e}") from e

        return statistics

    @asynccontextmanager
    async def transaction(self) -> AsyncGenerator[Any, None]:
        """
        Async context manager for database transactions.

        Yields:
            Database session with transaction support
        """
        session = self.session_factory()
        try:
            yield session
            session.commit()
            logger.debug("Transaction committed successfully")
        except Exception as e:
            session.rollback()
            logger.error(f"Transaction rolled back due to error: {e}")
            raise
        finally:
            session.close()

    async def backup_table_data(self, table_name: str, backup_path: Path) -> dict[str, Any]:
        """
        Create a backup of table data to local storage.
        
        Args:
            table_name: Name of the table to backup
            backup_path: Path to store backup files
            
        Returns:
            Backup metadata and statistics
        """
        try:
            backup_path.mkdir(parents=True, exist_ok=True)

            with self.engine.connect() as conn:
                # Export table data to CSV
                result = conn.execute(text(f"SELECT * FROM {table_name}"))
                df = pd.DataFrame(result.fetchall(), columns=result.keys())

                timestamp = pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')
                backup_file = backup_path / f"{table_name}_backup_{timestamp}.csv"

                df.to_csv(backup_file, index=False)

                backup_metadata = {
                    'table_name': table_name,
                    'backup_file': str(backup_file),
                    'record_count': len(df),
                    'file_size': backup_file.stat().st_size,
                    'backup_timestamp': pd.Timestamp.now().isoformat(),
                    'status': 'completed'
                }

                logger.info(f"Table backup completed: {backup_metadata}")
                return backup_metadata

        except Exception as e:
            logger.error(f"Backup failed for table {table_name}: {e}")
            return {
                'table_name': table_name,
                'status': 'failed',
                'error': str(e),
                'backup_timestamp': pd.Timestamp.now().isoformat()
            }

    async def restore_table_data(self, backup_file: Path, table_name: str) -> dict[str, Any]:
        """
        Restore table data from backup file.
        
        Args:
            backup_file: Path to backup CSV file
            table_name: Name of target table
            
        Returns:
            Restore operation metadata
        """
        try:
            if not backup_file.exists():
                raise FileNotFoundError(f"Backup file not found: {backup_file}")

            df = pd.read_csv(backup_file)

            with self.engine.connect() as conn:
                # Clear existing data (optional - could be parameterized)
                conn.execute(text(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE"))

                # Insert backup data
                df.to_sql(table_name, conn, if_exists='append', index=False)
                conn.commit()

                restore_metadata = {
                    'table_name': table_name,
                    'backup_file': str(backup_file),
                    'records_restored': len(df),
                    'restore_timestamp': pd.Timestamp.now().isoformat(),
                    'status': 'completed'
                }

                logger.info(f"Table restore completed: {restore_metadata}")
                return restore_metadata

        except Exception as e:
            logger.error(f"Restore failed for table {table_name}: {e}")
            return {
                'table_name': table_name,
                'status': 'failed',
                'error': str(e),
                'restore_timestamp': pd.Timestamp.now().isoformat()
            }

    async def create_full_backup(self, backup_dir: str = "backups/supabase") -> dict[str, Any]:
        """Create full backup of all star schema tables."""
        backup_path = Path(backup_dir)
        backup_summary = {
            'backup_timestamp': pd.Timestamp.now().isoformat(),
            'backup_directory': str(backup_path),
            'tables_backed_up': [],
            'total_records': 0,
            'total_size': 0,
            'status': 'completed',
            'errors': []
        }

        tables = ["fact_sale", "dim_product", "dim_customer", "dim_country", "dim_invoice", "dim_date"]

        for table in tables:
            try:
                result = await self.backup_table_data(table, backup_path)
                if result['status'] == 'completed':
                    backup_summary['tables_backed_up'].append(result)
                    backup_summary['total_records'] += result['record_count']
                    backup_summary['total_size'] += result['file_size']
                else:
                    backup_summary['errors'].append(result)
            except Exception as e:
                backup_summary['errors'].append({
                    'table_name': table,
                    'error': str(e)
                })

        if backup_summary['errors']:
            backup_summary['status'] = 'partial'

        logger.info(f"Full backup completed: {len(backup_summary['tables_backed_up'])} tables, {backup_summary['total_records']} records")
        return backup_summary

    async def cleanup_old_connections(self) -> None:
        """Clean up old database connections."""
        try:
            if self._engine:
                self._engine.dispose()
                logger.info("Database connections cleaned up")
        except Exception as e:
            logger.warning(f"Error during connection cleanup: {e}")


# Global Supabase client instance
_supabase_client: SupabaseClient | None = None


def get_supabase_client() -> SupabaseClient:
    """
    Get the global Supabase client instance.

    Returns:
        Configured SupabaseClient instance
    """
    global _supabase_client
    if _supabase_client is None:
        _supabase_client = SupabaseClient()
    return _supabase_client


async def health_check_supabase() -> dict[str, Any]:
    """
    Perform a comprehensive health check of the Supabase connection.

    Returns:
        Dict containing health check results
    """
    client = get_supabase_client()

    try:
        # Test connection
        connection_info = await client.test_connection()

        # Get table statistics
        statistics = await client.get_table_statistics()

        # Validate data integrity
        integrity_report = await client.validate_data_integrity()

        return {
            "status": "healthy",
            "connection": connection_info,
            "tables": statistics,
            "integrity": integrity_report,
            "timestamp": pd.Timestamp.now().isoformat()
        }

    except Exception as e:
        logger.error(f"Supabase health check failed: {e}")
        return {
            "status": "unhealthy",
            "error": str(e),
            "timestamp": pd.Timestamp.now().isoformat()
        }
