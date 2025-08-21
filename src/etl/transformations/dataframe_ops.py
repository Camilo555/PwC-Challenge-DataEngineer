"""
Centralized DataFrame Transformation Utilities
Provides consistent DataFrame API operations across all ETL processes.
"""
from typing import List, Dict, Any, Optional, Union
from datetime import datetime
import pandas as pd

try:
    import polars as pl
    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False

try:
    from pyspark.sql import DataFrame as SparkDataFrame, SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType
    from pyspark.sql.window import Window
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False

from core.logging import get_logger

logger = get_logger(__name__)


class DataFrameTransformer:
    """
    Unified DataFrame transformation utilities supporting Pandas, Polars, and Spark.
    All transformations use DataFrame APIs instead of SQL for consistency.
    """
    
    def __init__(self, engine: str = "pandas"):
        """Initialize transformer with specified engine."""
        self.engine = engine.lower()
        
        if self.engine == "spark" and not SPARK_AVAILABLE:
            raise ImportError("Spark not available. Install pyspark.")
        if self.engine == "polars" and not POLARS_AVAILABLE:
            raise ImportError("Polars not available. Install polars.")
    
    def clean_sales_data(self, df: Union[pd.DataFrame, 'pl.DataFrame', 'SparkDataFrame']) -> Union[pd.DataFrame, 'pl.DataFrame', 'SparkDataFrame']:
        """
        Clean sales data using DataFrame API operations.
        Replaces SQL-based transformations with DataFrame operations.
        """
        if self.engine == "pandas":
            return self._clean_sales_pandas(df)
        elif self.engine == "polars":
            return self._clean_sales_polars(df)
        elif self.engine == "spark":
            return self._clean_sales_spark(df)
        else:
            raise ValueError(f"Unsupported engine: {self.engine}")
    
    def _clean_sales_pandas(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean sales data using Pandas DataFrame API."""
        logger.info("Cleaning sales data with Pandas DataFrame API")
        
        # Filter valid transactions (no SQL WHERE)
        df_filtered = df[
            (df['quantity'] > 0) &
            (df['unit_price'] > 0) &
            (~df['invoice_no'].str.startswith('C', na=False)) &
            (df['invoice_no'].notna())
        ].copy()
        
        # Data type conversions
        df_filtered['quantity'] = pd.to_numeric(df_filtered['quantity'], errors='coerce')
        df_filtered['unit_price'] = pd.to_numeric(df_filtered['unit_price'], errors='coerce')
        
        # Calculate derived fields
        df_filtered['total_amount'] = df_filtered['quantity'] * df_filtered['unit_price']
        
        # Handle missing values
        df_filtered['customer_id'] = df_filtered['customer_id'].fillna('UNKNOWN')
        df_filtered['description'] = df_filtered['description'].fillna('No Description')
        
        # Remove duplicates using DataFrame operations
        df_cleaned = df_filtered.drop_duplicates(
            subset=['invoice_no', 'stock_code', 'quantity', 'unit_price'],
            keep='first'
        )
        
        # Add metadata
        df_cleaned['processed_at'] = datetime.utcnow()
        df_cleaned['data_quality_score'] = self._calculate_quality_score_pandas(df_cleaned)
        
        return df_cleaned
    
    def _clean_sales_polars(self, df: 'pl.DataFrame') -> 'pl.DataFrame':
        """Clean sales data using Polars DataFrame API."""
        if not POLARS_AVAILABLE:
            raise ImportError("Polars not available")
            
        logger.info("Cleaning sales data with Polars DataFrame API")
        
        # Filter and transform using Polars expressions
        df_cleaned = (df
            .filter(
                (pl.col('quantity') > 0) &
                (pl.col('unit_price') > 0) &
                (~pl.col('invoice_no').str.starts_with('C')) &
                (pl.col('invoice_no').is_not_null())
            )
            .with_columns([
                pl.col('quantity').cast(pl.Int64),
                pl.col('unit_price').cast(pl.Float64),
                (pl.col('quantity') * pl.col('unit_price')).alias('total_amount'),
                pl.col('customer_id').fill_null('UNKNOWN'),
                pl.col('description').fill_null('No Description'),
                pl.lit(datetime.utcnow()).alias('processed_at')
            ])
            .unique(subset=['invoice_no', 'stock_code', 'quantity', 'unit_price'])
        )
        
        return df_cleaned
    
    def _clean_sales_spark(self, df: 'SparkDataFrame') -> 'SparkDataFrame':
        """Clean sales data using Spark DataFrame API."""
        if not SPARK_AVAILABLE:
            raise ImportError("Spark not available")
            
        logger.info("Cleaning sales data with Spark DataFrame API")
        
        # Filter using DataFrame API (not SQL)
        df_filtered = df.filter(
            (F.col('quantity') > 0) &
            (F.col('unit_price') > 0) &
            (~F.col('invoice_no').startswith('C')) &
            (F.col('invoice_no').isNotNull())
        )
        
        # Add calculated columns
        df_transformed = df_filtered.withColumns({
            'quantity': F.col('quantity').cast('integer'),
            'unit_price': F.col('unit_price').cast('double'),
            'total_amount': F.col('quantity') * F.col('unit_price'),
            'customer_id': F.when(F.col('customer_id').isNull(), 'UNKNOWN').otherwise(F.col('customer_id')),
            'description': F.when(F.col('description').isNull(), 'No Description').otherwise(F.col('description')),
            'processed_at': F.current_timestamp()
        })
        
        # Remove duplicates using DataFrame API
        window_spec = Window.partitionBy('invoice_no', 'stock_code', 'quantity', 'unit_price').orderBy(F.col('invoice_date'))
        df_deduplicated = df_transformed.withColumn('row_number', F.row_number().over(window_spec)).filter(F.col('row_number') == 1).drop('row_number')
        
        return df_deduplicated
    
    def aggregate_by_dimensions(self, df: Union[pd.DataFrame, 'pl.DataFrame', 'SparkDataFrame'], group_cols: List[str]) -> Union[pd.DataFrame, 'pl.DataFrame', 'SparkDataFrame']:
        """
        Aggregate data by dimensions using DataFrame API.
        Replaces SQL GROUP BY with DataFrame groupBy operations.
        """
        if self.engine == "pandas":
            return self._aggregate_pandas(df, group_cols)
        elif self.engine == "polars":
            return self._aggregate_polars(df, group_cols)
        elif self.engine == "spark":
            return self._aggregate_spark(df, group_cols)
    
    def _aggregate_pandas(self, df: pd.DataFrame, group_cols: List[str]) -> pd.DataFrame:
        """Aggregate using Pandas groupby."""
        return df.groupby(group_cols).agg({
            'total_amount': ['sum', 'mean', 'count'],
            'quantity': 'sum',
            'customer_id': 'nunique'
        }).round(2)
    
    def _aggregate_polars(self, df: 'pl.DataFrame', group_cols: List[str]) -> 'pl.DataFrame':
        """Aggregate using Polars group_by."""
        return df.group_by(group_cols).agg([
            pl.col('total_amount').sum().alias('total_revenue'),
            pl.col('total_amount').mean().alias('avg_amount'),
            pl.col('total_amount').count().alias('transaction_count'),
            pl.col('quantity').sum().alias('total_quantity'),
            pl.col('customer_id').n_unique().alias('unique_customers')
        ])
    
    def _aggregate_spark(self, df: 'SparkDataFrame', group_cols: List[str]) -> 'SparkDataFrame':
        """Aggregate using Spark groupBy."""
        return df.groupBy(*group_cols).agg(
            F.sum('total_amount').alias('total_revenue'),
            F.mean('total_amount').alias('avg_amount'),
            F.count('total_amount').alias('transaction_count'),
            F.sum('quantity').alias('total_quantity'),
            F.countDistinct('customer_id').alias('unique_customers')
        )
    
    def window_operations(self, df: Union[pd.DataFrame, 'pl.DataFrame', 'SparkDataFrame'], partition_cols: List[str], order_cols: List[str]) -> Union[pd.DataFrame, 'pl.DataFrame', 'SparkDataFrame']:
        """
        Perform window operations using DataFrame API.
        Replaces SQL window functions with DataFrame window operations.
        """
        if self.engine == "pandas":
            return self._window_pandas(df, partition_cols, order_cols)
        elif self.engine == "polars":
            return self._window_polars(df, partition_cols, order_cols)
        elif self.engine == "spark":
            return self._window_spark(df, partition_cols, order_cols)
    
    def _window_pandas(self, df: pd.DataFrame, partition_cols: List[str], order_cols: List[str]) -> pd.DataFrame:
        """Window operations using Pandas."""
        df_windowed = df.copy()
        
        # Add row number within partition
        df_windowed['row_number'] = df_windowed.groupby(partition_cols).cumcount() + 1
        
        # Add running totals
        df_windowed['running_total'] = df_windowed.groupby(partition_cols)['total_amount'].cumsum()
        
        # Add ranking
        df_windowed['amount_rank'] = df_windowed.groupby(partition_cols)['total_amount'].rank(method='dense', ascending=False)
        
        return df_windowed
    
    def _window_polars(self, df: 'pl.DataFrame', partition_cols: List[str], order_cols: List[str]) -> 'pl.DataFrame':
        """Window operations using Polars."""
        return df.with_columns([
            pl.col('total_amount').cumsum().over(partition_cols).alias('running_total'),
            pl.col('total_amount').rank(method='dense', descending=True).over(partition_cols).alias('amount_rank'),
            (pl.int_range(pl.len()) + 1).over(partition_cols).alias('row_number')
        ])
    
    def _window_spark(self, df: 'SparkDataFrame', partition_cols: List[str], order_cols: List[str]) -> 'SparkDataFrame':
        """Window operations using Spark."""
        window_spec = Window.partitionBy(*partition_cols).orderBy(*order_cols)
        
        return df.withColumns({
            'row_number': F.row_number().over(window_spec),
            'running_total': F.sum('total_amount').over(window_spec.rowsBetween(Window.unboundedPreceding, Window.currentRow)),
            'amount_rank': F.dense_rank().over(Window.partitionBy(*partition_cols).orderBy(F.col('total_amount').desc()))
        })
    
    def _calculate_quality_score_pandas(self, df: pd.DataFrame) -> float:
        """Calculate data quality score for Pandas DataFrame."""
        total_fields = len(df.columns) * len(df)
        non_null_fields = df.count().sum()
        return round(non_null_fields / total_fields, 4) if total_fields > 0 else 0.0
    
    def join_dataframes(self, left_df: Union[pd.DataFrame, 'pl.DataFrame', 'SparkDataFrame'], 
                       right_df: Union[pd.DataFrame, 'pl.DataFrame', 'SparkDataFrame'],
                       join_keys: List[str], join_type: str = "inner") -> Union[pd.DataFrame, 'pl.DataFrame', 'SparkDataFrame']:
        """
        Join DataFrames using DataFrame API instead of SQL JOIN.
        """
        if self.engine == "pandas":
            return left_df.merge(right_df, on=join_keys, how=join_type)
        elif self.engine == "polars":
            return left_df.join(right_df, on=join_keys, how=join_type)
        elif self.engine == "spark":
            join_condition = [left_df[key] == right_df[key] for key in join_keys]
            return left_df.join(right_df, on=join_condition, how=join_type)
    
    def pivot_operations(self, df: Union[pd.DataFrame, 'pl.DataFrame', 'SparkDataFrame'],
                        index_cols: List[str], value_col: str, agg_func: str = "sum") -> Union[pd.DataFrame, 'pl.DataFrame', 'SparkDataFrame']:
        """
        Pivot operations using DataFrame API instead of SQL PIVOT.
        """
        if self.engine == "pandas":
            return df.pivot_table(index=index_cols, values=value_col, aggfunc=agg_func, fill_value=0)
        elif self.engine == "polars":
            # Polars doesn't have direct pivot, use group_by with conditional aggregation
            return df.group_by(index_cols).agg([
                pl.col(value_col).sum().alias(f"{value_col}_sum")
            ])
        elif self.engine == "spark":
            return df.groupBy(*index_cols).pivot("category").agg(F.sum(value_col))