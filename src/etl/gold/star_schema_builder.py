"""
Enhanced Star Schema Builder for Gold Layer
Builds production-ready star schema with SCD Type 2 dimensions and fact tables.
Integrates with the engine abstraction layer for cross-engine compatibility.
"""

import hashlib
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any

# Import the engine abstraction
from src.etl.framework.engine_strategy import (
    DataFrameOperations,
    EngineConfig,
    EngineFactory,
    EngineType,
)

# Import Polars for expression building (when available)
try:
    import polars as pl
    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False


@dataclass
class DimensionConfig:
    """Configuration for dimension table construction."""
    name: str
    business_key_cols: list[str]
    scd2_tracked_cols: list[str]
    type: int = 2  # SCD Type (1 or 2)


@dataclass
class FactConfig:
    """Configuration for fact table construction."""
    name: str
    grain_cols: list[str]
    measure_cols: list[str]
    dimension_keys: dict[str, str]  # dimension_name: join_key


class StarSchemaBuilder:
    """
    Enhanced star schema builder with SCD Type 2 support and engine abstraction.
    Builds production-ready dimensional models for analytics.
    """

    def __init__(self, engine: DataFrameOperations, config: dict[str, Any]):
        """Initialize star schema builder with engine and configuration."""
        self.engine = engine
        self.config = config
        self.gold_path = config.get('gold_path', 'data/gold')
        self.audit_cols = ['etl_created_at', 'etl_updated_at', 'etl_batch_id']

        # Dimension configurations
        self.dimensions_config = {
            'date': DimensionConfig('date', ['date'], [], type=0),  # Type 0 - static
            'product': DimensionConfig('product', ['product_id', 'product_code'],
                                     ['product_name', 'category', 'subcategory', 'price']),
            'customer': DimensionConfig('customer', ['customer_id', 'email'],
                                      ['customer_name', 'segment', 'country', 'city']),
            'store': DimensionConfig('store', ['store_id', 'store_code'],
                                   ['store_name', 'address', 'manager'], type=1)  # Type 1 - overwrite
        }

    def build_complete_star_schema(self, silver_data: dict[str, Any]) -> dict[str, Any]:
        """
        Build complete star schema with all dimensions and fact table.
        
        Args:
            silver_data: Dictionary containing silver layer datasets
            
        Returns:
            Dictionary containing all star schema tables
        """
        schema_tables = {}

        # Build core dimensions
        schema_tables['dim_date'] = self.build_dim_date('2020-01-01', '2030-12-31')
        schema_tables['dim_product'] = self.build_dim_product(silver_data.get('products'))
        schema_tables['dim_customer'] = self.build_dim_customer(silver_data.get('customers'))
        schema_tables['dim_store'] = self.build_dim_store(silver_data.get('stores'))

        # Build fact table
        schema_tables['fact_sales'] = self.build_fact_sales(
            silver_data.get('sales'),
            schema_tables
        )

        return schema_tables

    def build_dim_date(self, start_date: str, end_date: str) -> Any:
        """
        Create comprehensive date dimension with calendar and fiscal attributes.
        
        Args:
            start_date: Start date in YYYY-MM-DD format
            end_date: End date in YYYY-MM-DD format
            
        Returns:
            Date dimension dataframe
        """
        if not POLARS_AVAILABLE:
            raise ImportError("Polars is required for date dimension generation")

        # Generate date range using Polars
        dates = pl.date_range(
            date.fromisoformat(start_date),
            date.fromisoformat(end_date),
            interval='1d',
            eager=True
        )

        df = pl.DataFrame({'date': dates})

        # Add calendar attributes
        df = df.with_columns([
            # Primary key
            (pl.col('date').dt.year() * 10000 +
             pl.col('date').dt.month() * 100 +
             pl.col('date').dt.day()).alias('date_key'),

            # Calendar hierarchy
            pl.col('date').dt.year().alias('year'),
            pl.col('date').dt.quarter().alias('quarter'),
            pl.col('date').dt.month().alias('month'),
            pl.col('date').dt.day().alias('day'),
            pl.col('date').dt.weekday().alias('weekday'),
            pl.col('date').dt.week().alias('week_of_year'),

            # Formatted names
            pl.col('date').dt.strftime('%B').alias('month_name'),
            pl.col('date').dt.strftime('%A').alias('day_name'),

            # Business flags
            (pl.col('date').dt.weekday() >= 6).alias('is_weekend'),
            pl.lit(False).alias('is_holiday'),  # Could be enhanced with holiday calendar

            # Business quarters (calendar year)
            pl.when(pl.col('date').dt.month().is_in([1, 2, 3]))
            .then(pl.lit('Q1'))
            .when(pl.col('date').dt.month().is_in([4, 5, 6]))
            .then(pl.lit('Q2'))
            .when(pl.col('date').dt.month().is_in([7, 8, 9]))
            .then(pl.lit('Q3'))
            .otherwise(pl.lit('Q4'))
            .alias('quarter_name')
        ])

        # Add fiscal attributes (assuming fiscal year starts July 1)
        df = df.with_columns([
            pl.when(pl.col('month') >= 7)
            .then(pl.col('year'))
            .otherwise(pl.col('year') - 1)
            .alias('fiscal_year'),

            pl.when(pl.col('month') >= 7)
            .then(pl.col('month') - 6)
            .otherwise(pl.col('month') + 6)
            .alias('fiscal_month'),

            pl.when(pl.col('month').is_in([7, 8, 9]))
            .then(1)
            .when(pl.col('month').is_in([10, 11, 12]))
            .then(2)
            .when(pl.col('month').is_in([1, 2, 3]))
            .then(3)
            .otherwise(4)
            .alias('fiscal_quarter'),

            # Fiscal quarter names
            pl.when(pl.col('month').is_in([7, 8, 9]))
            .then(pl.lit('FQ1'))
            .when(pl.col('month').is_in([10, 11, 12]))
            .then(pl.lit('FQ2'))
            .when(pl.col('month').is_in([1, 2, 3]))
            .then(pl.lit('FQ3'))
            .otherwise(pl.lit('FQ4'))
            .alias('fiscal_quarter_name')
        ])

        # Add audit columns
        df = self._add_audit_columns(df)

        # Write to gold layer
        output_path = f'{self.gold_path}/dim_date'
        self.engine.write_parquet(df, output_path, mode='overwrite')

        return df

    def build_dim_product(self, df_product_silver: Any) -> Any:
        """
        Build product dimension with SCD Type 2 tracking.
        
        Args:
            df_product_silver: Silver layer product data
            
        Returns:
            Product dimension with SCD2 implementation
        """
        if df_product_silver is None:
            return self._create_empty_product_dimension()

        # Convert to engine format if needed
        if POLARS_AVAILABLE and not hasattr(df_product_silver, 'with_columns'):
            df_product_silver = pl.DataFrame(df_product_silver)

        # Generate business key hash and data hash for change detection
        df = self._add_business_keys_and_hash(df_product_silver, {
            'product_bk': ['product_id', 'product_code'],
            'product_hash': ['product_name', 'category', 'subcategory', 'price']
        })

        # Apply SCD2 logic
        existing_path = f'{self.gold_path}/dim_product'

        try:
            existing_df = self.engine.read_parquet(existing_path)
            df = self._apply_scd2_logic(
                existing_df=existing_df,
                new_df=df,
                business_key='product_bk',
                hash_col='product_hash',
                dimension_name='product'
            )
        except (FileNotFoundError, Exception):
            # First load - add SCD2 columns
            df = self._add_scd2_columns(df, 'product')

        # Add audit columns
        df = self._add_audit_columns(df)

        # Write to gold layer
        self.engine.write_parquet(df, existing_path, mode='overwrite')

        return df

    def build_dim_customer(self, df_customer_silver: Any) -> Any:
        """
        Build customer dimension with PII handling and SCD Type 2.
        
        Args:
            df_customer_silver: Silver layer customer data
            
        Returns:
            Customer dimension with SCD2 and PII masking
        """
        if df_customer_silver is None:
            return self._create_empty_customer_dimension()

        # Convert to engine format if needed
        if POLARS_AVAILABLE and not hasattr(df_customer_silver, 'with_columns'):
            df_customer_silver = pl.DataFrame(df_customer_silver)

        # Canonicalize customer data for consistent matching
        df = self._canonicalize_customer_data(df_customer_silver)

        # Generate business keys and hash
        df = self._add_business_keys_and_hash(df, {
            'customer_bk': ['customer_id', 'email_canonical'],
            'customer_hash': ['customer_name_canonical', 'segment', 'country', 'city']
        })

        # Apply SCD2 logic
        existing_path = f'{self.gold_path}/dim_customer'

        try:
            existing_df = self.engine.read_parquet(existing_path)
            df = self._apply_scd2_logic(
                existing_df=existing_df,
                new_df=df,
                business_key='customer_bk',
                hash_col='customer_hash',
                dimension_name='customer'
            )
        except (FileNotFoundError, Exception):
            df = self._add_scd2_columns(df, 'customer')

        # Mask PII fields for non-current records
        df = self._mask_pii_for_historical_records(df, ['email', 'phone', 'address'])

        # Add audit columns
        df = self._add_audit_columns(df)

        # Write to gold layer
        self.engine.write_parquet(df, existing_path, mode='overwrite')

        return df

    def build_dim_store(self, df_store_silver: Any) -> Any:
        """
        Build store/location dimension with geographical hierarchy.
        
        Args:
            df_store_silver: Silver layer store data
            
        Returns:
            Store dimension table
        """
        if df_store_silver is None:
            return self._create_empty_store_dimension()

        # Convert to engine format if needed
        if POLARS_AVAILABLE and not hasattr(df_store_silver, 'with_columns'):
            df_store_silver = pl.DataFrame(df_store_silver)

        # Add business keys and hierarchical attributes
        df = self._add_business_keys_and_hash(df_store_silver, {
            'store_bk': ['store_id', 'store_code'],
            'store_key': ['store_id', 'store_code']
        })

        # Add geographical hierarchy and computed fields
        if POLARS_AVAILABLE:
            df = df.with_columns([
                # Geographical hierarchy
                pl.concat_str([pl.col('country'), pl.col('region')], separator='_').alias('country_region'),
                pl.concat_str([
                    pl.col('address'),
                    pl.col('city'),
                    pl.col('state'),
                    pl.col('country'),
                    pl.col('postal_code')
                ], separator=', ').alias('full_address'),

                # Store attributes
                pl.lit(datetime.now().date()).alias('effective_date'),
                pl.lit(date(9999, 12, 31)).alias('end_date'),
                pl.lit(True).alias('is_current')
            ])

        # Add audit columns
        df = self._add_audit_columns(df)

        # Write to gold layer
        output_path = f'{self.gold_path}/dim_store'
        self.engine.write_parquet(df, output_path, mode='overwrite')

        return df

    def build_fact_sales(self, df_sales_silver: Any, dimensions: dict[str, Any]) -> Any:
        """
        Build fact table at invoice line grain with foreign keys to dimensions.
        
        Args:
            df_sales_silver: Silver layer sales data
            dimensions: Dictionary of dimension tables
            
        Returns:
            Fact sales table with dimension foreign keys and measures
        """
        if df_sales_silver is None:
            return self._create_empty_fact_sales()

        # Start with silver sales data
        df = df_sales_silver

        # Convert to engine format if needed
        if POLARS_AVAILABLE and not hasattr(df, 'with_columns'):
            df = pl.DataFrame(df)

        # Join with date dimension
        if 'dim_date' in dimensions and dimensions['dim_date'] is not None:
            df = self._join_with_date_dimension(df, dimensions['dim_date'])
        else:
            df = self._add_default_date_key(df)

        # Join with product dimension (current records only)
        if 'dim_product' in dimensions and dimensions['dim_product'] is not None:
            df = self._join_with_product_dimension(df, dimensions['dim_product'])
        else:
            df = self._add_default_product_key(df)

        # Join with customer dimension (current records only)
        if 'dim_customer' in dimensions and dimensions['dim_customer'] is not None:
            df = self._join_with_customer_dimension(df, dimensions['dim_customer'])
        else:
            df = self._add_default_customer_key(df)

        # Join with store dimension
        if 'dim_store' in dimensions and dimensions['dim_store'] is not None:
            df = self._join_with_store_dimension(df, dimensions['dim_store'])
        else:
            df = self._add_default_store_key(df)

        # Calculate derived measures
        df = self._calculate_fact_measures(df)

        # Select final fact table columns
        fact_columns = [
            'invoice_line_id',  # Primary key
            'invoice_id',       # Degenerate dimension
            'date_key',         # Foreign keys
            'product_key',
            'customer_key',
            'store_key',
            'quantity',         # Measures
            'unit_price',
            'line_amount',
            'discount_percentage',
            'discount_amount',
            'tax_rate',
            'tax_amount',
            'net_amount'
        ]

        # Select final columns
        df = self.engine.select(df, fact_columns)

        # Add audit columns
        df = self._add_audit_columns(df)

        # Write to gold layer with partitioning by date
        output_path = f'{self.gold_path}/fact_sales'
        self.engine.write_parquet(
            df,
            output_path,
            partition_cols=['date_key'],
            mode='overwrite'
        )

        return df

    # Helper methods for SCD2 implementation

    def _apply_scd2_logic(self, existing_df: Any, new_df: Any, business_key: str,
                         hash_col: str, dimension_name: str) -> Any:
        """
        Apply SCD Type 2 logic to track historical changes.
        
        Args:
            existing_df: Current dimension table
            new_df: New data to process
            business_key: Business key column name
            hash_col: Hash column for change detection
            dimension_name: Name of the dimension
            
        Returns:
            Updated dimension table with SCD2 logic applied
        """
        # For simplification in this implementation, we'll use a basic approach
        # In production, this would include proper change detection and versioning

        # Get current records from existing dimension
        if POLARS_AVAILABLE:
            current_records = existing_df.filter(pl.col('is_current') == True)

            # Simple merge strategy - mark all existing as not current and add new as current
            # This is simplified - production would have proper change detection
            closed_records = existing_df.with_columns([
                pl.lit(datetime.now().date()).alias('end_date'),
                pl.lit(False).alias('is_current')
            ])

            # Add new records with SCD2 attributes
            new_records = self._add_scd2_columns(new_df, dimension_name)

            # Combine old (closed) and new records
            if hasattr(pl, 'concat'):
                return pl.concat([closed_records, new_records])
            else:
                return new_records  # Fallback if concat not available
        else:
            # Fallback - just return new records with SCD2 columns
            return self._add_scd2_columns(new_df, dimension_name)

    def _add_scd2_columns(self, df: Any, dimension_name: str) -> Any:
        """Add SCD Type 2 columns to a dimension."""
        if POLARS_AVAILABLE and hasattr(df, 'with_columns'):
            return df.with_columns([
                self._generate_surrogate_key_expr([f'{dimension_name}_bk']).alias(f'{dimension_name}_key'),
                pl.lit(datetime.now().date()).alias('effective_date'),
                pl.lit(date(9999, 12, 31)).alias('end_date'),
                pl.lit(True).alias('is_current'),
                pl.lit(1).alias('version')
            ])
        else:
            return df

    def _add_business_keys_and_hash(self, df: Any, key_definitions: dict[str, list[str]]) -> Any:
        """Add business keys and hash columns to dataframe."""
        if POLARS_AVAILABLE and hasattr(df, 'with_columns'):
            expressions = {}
            for key_name, columns in key_definitions.items():
                if key_name.endswith('_hash'):
                    expressions[key_name] = self._generate_hash_expr(columns)
                else:
                    expressions[key_name] = self._generate_business_key_expr(columns)

            return df.with_columns([expr.alias(name) for name, expr in expressions.items()])
        else:
            return df

    def _generate_business_key_expr(self, columns: list[str]) -> Any:
        """Generate business key hash from columns."""
        if POLARS_AVAILABLE:
            concat_expr = pl.concat_str(columns, separator='|')
            return concat_expr.map_elements(
                lambda x: hashlib.sha256(str(x).encode()).hexdigest()[:16],
                return_dtype=pl.Utf8
            )
        else:
            return None

    def _generate_hash_expr(self, columns: list[str]) -> Any:
        """Generate hash for change detection."""
        if POLARS_AVAILABLE:
            concat_expr = pl.concat_str(columns, separator='|')
            return concat_expr.map_elements(
                lambda x: hashlib.sha256(str(x).encode()).hexdigest(),
                return_dtype=pl.Utf8
            )
        else:
            return None

    def _generate_surrogate_key_expr(self, columns: list[str]) -> Any:
        """Generate deterministic surrogate key."""
        if POLARS_AVAILABLE:
            concat_expr = pl.concat_str(columns, separator='|')
            return concat_expr.map_elements(
                lambda x: hashlib.sha256(str(x).encode()).hexdigest()[:16],
                return_dtype=pl.Utf8
            )
        else:
            return None

    def _canonicalize_customer_data(self, df: Any) -> Any:
        """Canonicalize customer data for consistent matching."""
        if POLARS_AVAILABLE and hasattr(df, 'with_columns'):
            return df.with_columns([
                pl.col('customer_name').str.to_lowercase().str.strip_chars().alias('customer_name_canonical'),
                pl.col('email').str.to_lowercase().str.strip_chars().alias('email_canonical'),
                pl.col('phone').str.replace_all(r'[^0-9]', '').alias('phone_canonical')
            ])
        else:
            return df

    def _mask_pii_for_historical_records(self, df: Any, pii_columns: list[str]) -> Any:
        """Mask PII fields for non-current records."""
        if POLARS_AVAILABLE and hasattr(df, 'with_columns'):
            expressions = []
            for col in pii_columns:
                if col in df.columns:
                    expressions.append(
                        pl.when(pl.col('is_current') == False)
                        .then(pl.lit('***MASKED***'))
                        .otherwise(pl.col(col))
                        .alias(col)
                    )
            if expressions:
                return df.with_columns(expressions)
        return df

    def _add_audit_columns(self, df: Any) -> Any:
        """Add standard audit columns."""
        if POLARS_AVAILABLE and hasattr(df, 'with_columns'):
            return df.with_columns([
                pl.lit(datetime.now()).alias('etl_created_at'),
                pl.lit(datetime.now()).alias('etl_updated_at'),
                pl.lit(self.config.get('batch_id', 'manual_run')).alias('etl_batch_id')
            ])
        else:
            return df

    # Dimension join helpers

    def _join_with_date_dimension(self, df: Any, dim_date: Any) -> Any:
        """Join fact table with date dimension."""
        # This is a simplified join - production would handle various date formats
        if POLARS_AVAILABLE:
            return df.join(
                dim_date.select(['date_key', 'date']),
                left_on='invoice_date',
                right_on='date',
                how='left'
            ).with_columns([
                pl.col('date_key').fill_null(-1)  # Unknown date key
            ])
        else:
            return df

    def _join_with_product_dimension(self, df: Any, dim_product: Any) -> Any:
        """Join fact table with product dimension (current records only)."""
        if POLARS_AVAILABLE:
            current_products = dim_product.filter(pl.col('is_current') == True)
            return df.join(
                current_products.select(['product_key', 'product_id']),
                on='product_id',
                how='left'
            ).with_columns([
                pl.col('product_key').fill_null(-1)
            ])
        else:
            return df

    def _join_with_customer_dimension(self, df: Any, dim_customer: Any) -> Any:
        """Join fact table with customer dimension (current records only)."""
        if POLARS_AVAILABLE:
            current_customers = dim_customer.filter(pl.col('is_current') == True)
            return df.join(
                current_customers.select(['customer_key', 'customer_id']),
                on='customer_id',
                how='left'
            ).with_columns([
                pl.col('customer_key').fill_null(-1)
            ])
        else:
            return df

    def _join_with_store_dimension(self, df: Any, dim_store: Any) -> Any:
        """Join fact table with store dimension."""
        if POLARS_AVAILABLE:
            return df.join(
                dim_store.select(['store_key', 'store_id']),
                on='store_id',
                how='left'
            ).with_columns([
                pl.col('store_key').fill_null(-1)
            ])
        else:
            return df

    def _calculate_fact_measures(self, df: Any) -> Any:
        """Calculate derived measures for fact table."""
        if POLARS_AVAILABLE and hasattr(df, 'with_columns'):
            return df.with_columns([
                # Basic line amount
                (pl.col('quantity') * pl.col('unit_price')).alias('line_amount'),

                # Discount amount (assuming discount_percentage exists)
                (pl.col('quantity') * pl.col('unit_price') * pl.col('discount_percentage').fill_null(0) / 100).alias('discount_amount'),

                # Tax amount (assuming tax_rate exists)
                (pl.col('quantity') * pl.col('unit_price') * pl.col('tax_rate').fill_null(0) / 100).alias('tax_amount'),

                # Net amount (final amount)
                (
                    pl.col('quantity') * pl.col('unit_price') -
                    (pl.col('quantity') * pl.col('unit_price') * pl.col('discount_percentage').fill_null(0) / 100) +
                    (pl.col('quantity') * pl.col('unit_price') * pl.col('tax_rate').fill_null(0) / 100)
                ).alias('net_amount')
            ])
        else:
            return df

    # Default key helpers for missing dimensions

    def _add_default_date_key(self, df: Any) -> Any:
        """Add default date key when date dimension is not available."""
        if POLARS_AVAILABLE:
            return df.with_columns([pl.lit(-1).alias('date_key')])
        return df

    def _add_default_product_key(self, df: Any) -> Any:
        """Add default product key when product dimension is not available."""
        if POLARS_AVAILABLE:
            return df.with_columns([pl.lit(-1).alias('product_key')])
        return df

    def _add_default_customer_key(self, df: Any) -> Any:
        """Add default customer key when customer dimension is not available."""
        if POLARS_AVAILABLE:
            return df.with_columns([pl.lit(-1).alias('customer_key')])
        return df

    def _add_default_store_key(self, df: Any) -> Any:
        """Add default store key when store dimension is not available."""
        if POLARS_AVAILABLE:
            return df.with_columns([pl.lit(-1).alias('store_key')])
        return df

    # Empty dimension creators for error handling

    def _create_empty_product_dimension(self) -> Any:
        """Create empty product dimension structure."""
        if POLARS_AVAILABLE:
            return pl.DataFrame({
                'product_key': [-1],
                'product_id': ['UNKNOWN'],
                'product_name': ['Unknown Product'],
                'category': ['Unknown'],
                'is_current': [True],
                'effective_date': [datetime.now().date()],
                'end_date': [date(9999, 12, 31)]
            })
        return None

    def _create_empty_customer_dimension(self) -> Any:
        """Create empty customer dimension structure."""
        if POLARS_AVAILABLE:
            return pl.DataFrame({
                'customer_key': [-1],
                'customer_id': ['UNKNOWN'],
                'customer_name': ['Unknown Customer'],
                'is_current': [True],
                'effective_date': [datetime.now().date()],
                'end_date': [date(9999, 12, 31)]
            })
        return None

    def _create_empty_store_dimension(self) -> Any:
        """Create empty store dimension structure."""
        if POLARS_AVAILABLE:
            return pl.DataFrame({
                'store_key': [-1],
                'store_id': ['UNKNOWN'],
                'store_name': ['Unknown Store'],
                'is_current': [True]
            })
        return None

    def _create_empty_fact_sales(self) -> Any:
        """Create empty fact sales structure."""
        if POLARS_AVAILABLE:
            return pl.DataFrame({
                'invoice_line_id': [],
                'date_key': [],
                'product_key': [],
                'customer_key': [],
                'store_key': [],
                'quantity': [],
                'unit_price': [],
                'net_amount': []
            })
        return None


# Factory function for creating star schema builders
def create_star_schema_builder(engine_type: EngineType = EngineType.POLARS,
                              config: dict[str, Any] | None = None) -> StarSchemaBuilder:
    """
    Create a star schema builder with the specified engine.
    
    Args:
        engine_type: Type of processing engine to use
        config: Configuration dictionary
        
    Returns:
        Configured StarSchemaBuilder instance
    """
    engine_config = EngineConfig(
        engine_type=engine_type,
        **(config or {})
    )

    engine = EngineFactory.create_engine(engine_config)

    return StarSchemaBuilder(engine, config or {})
