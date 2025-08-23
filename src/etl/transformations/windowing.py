"""
Window Functions for ETL Transformations
Provides engine-agnostic window operations for SCD2 and analytics.
"""
from typing import Any

import pandas as pd

try:
    import polars as pl
    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False

try:
    from pyspark.sql import DataFrame as SparkDataFrame
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False


def apply_scd2_window(df: Any, partition_key: str, order_key: str, engine: str = 'pandas') -> Any:
    """
    Apply SCD2 window functions to detect changes and manage validity periods.
    
    Args:
        df: Input DataFrame
        partition_key: Column to partition by (e.g., 'customer_id')
        order_key: Column to order by (e.g., 'valid_from')
        engine: Engine type ('pandas', 'polars', 'spark')
    
    Returns:
        DataFrame with SCD2 window columns added
    """
    if engine == 'pandas':
        return _apply_scd2_window_pandas(df, partition_key, order_key)
    elif engine == 'polars' and POLARS_AVAILABLE:
        return _apply_scd2_window_polars(df, partition_key, order_key)
    elif engine == 'spark' and SPARK_AVAILABLE:
        return _apply_scd2_window_spark(df, partition_key, order_key)
    else:
        raise ValueError(f"Engine '{engine}' not available or not supported")


def _apply_scd2_window_pandas(df: pd.DataFrame, partition_key: str, order_key: str) -> pd.DataFrame:
    """Apply SCD2 window functions using pandas."""
    df = df.copy()

    # Sort by partition key and order key
    df = df.sort_values([partition_key, order_key])

    # Create window functions
    df['prev_hash'] = df.groupby(partition_key)['attribute_hash'].shift(1)
    df['next_valid_from'] = df.groupby(partition_key)[order_key].shift(-1)
    df['row_number'] = df.groupby(partition_key).cumcount() + 1
    df['version'] = df['row_number']

    # Calculate if this is the current record
    df['is_current'] = df.groupby(partition_key)['row_number'].transform('max') == df['row_number']

    # Set valid_to date
    df['valid_to'] = df['next_valid_from']
    df.loc[df['is_current'], 'valid_to'] = None

    return df


def _apply_scd2_window_polars(df: pl.DataFrame | pl.LazyFrame,
                             partition_key: str, order_key: str) -> pl.DataFrame | pl.LazyFrame:
    """Apply SCD2 window functions using Polars."""
    return df.with_columns([
        # Previous hash for change detection
        pl.col('attribute_hash').shift(1).over(partition_key, order_by=order_key).alias('prev_hash'),

        # Next valid_from for setting valid_to
        pl.col(order_key).shift(-1).over(partition_key, order_by=order_key).alias('next_valid_from'),

        # Row number and version
        pl.int_range(pl.len()).over(partition_key, order_by=order_key).alias('row_number'),

        # Version number
        (pl.int_range(pl.len()) + 1).over(partition_key, order_by=order_key).alias('version'),

        # Is current record
        (pl.int_range(pl.len()) == pl.count().over(partition_key) - 1).over(partition_key, order_by=order_key).alias('is_current')
    ]).with_columns([
        # Set valid_to
        pl.when(pl.col('is_current')).then(None).otherwise(pl.col('next_valid_from')).alias('valid_to')
    ])


def _apply_scd2_window_spark(df: SparkDataFrame, partition_key: str, order_key: str) -> SparkDataFrame:
    """Apply SCD2 window functions using Spark."""
    window_spec = Window.partitionBy(partition_key).orderBy(order_key)
    window_spec_desc = Window.partitionBy(partition_key).orderBy(F.desc(order_key))

    return df.withColumns({
        'prev_hash': F.lag('attribute_hash', 1).over(window_spec),
        'next_valid_from': F.lead(order_key, 1).over(window_spec),
        'row_number': F.row_number().over(window_spec),
        'version': F.row_number().over(window_spec),
        'is_current': F.row_number().over(window_spec_desc) == 1
    }).withColumn(
        'valid_to',
        F.when(F.col('is_current'), None).otherwise(F.col('next_valid_from'))
    )


def calculate_running_totals(df: Any, partition_cols: list[str], order_col: str,
                           sum_col: str, engine: str = 'pandas') -> Any:
    """
    Calculate running totals within partitions.
    
    Args:
        df: Input DataFrame
        partition_cols: Columns to partition by
        order_col: Column to order by
        sum_col: Column to sum
        engine: Engine type ('pandas', 'polars', 'spark')
    
    Returns:
        DataFrame with running total column added
    """
    if engine == 'pandas':
        return _calculate_running_totals_pandas(df, partition_cols, order_col, sum_col)
    elif engine == 'polars' and POLARS_AVAILABLE:
        return _calculate_running_totals_polars(df, partition_cols, order_col, sum_col)
    elif engine == 'spark' and SPARK_AVAILABLE:
        return _calculate_running_totals_spark(df, partition_cols, order_col, sum_col)
    else:
        raise ValueError(f"Engine '{engine}' not available or not supported")


def _calculate_running_totals_pandas(df: pd.DataFrame, partition_cols: list[str],
                                   order_col: str, sum_col: str) -> pd.DataFrame:
    """Calculate running totals using pandas."""
    df = df.copy()
    df = df.sort_values(partition_cols + [order_col])
    df[f'{sum_col}_running_total'] = df.groupby(partition_cols)[sum_col].cumsum()
    return df


def _calculate_running_totals_polars(df: pl.DataFrame | pl.LazyFrame,
                                   partition_cols: list[str], order_col: str, sum_col: str) -> pl.DataFrame | pl.LazyFrame:
    """Calculate running totals using Polars."""
    return df.with_columns([
        pl.col(sum_col).cumsum().over(partition_cols, order_by=order_col).alias(f'{sum_col}_running_total')
    ])


def _calculate_running_totals_spark(df: SparkDataFrame, partition_cols: list[str],
                                  order_col: str, sum_col: str) -> SparkDataFrame:
    """Calculate running totals using Spark."""
    window_spec = Window.partitionBy(*partition_cols).orderBy(order_col) \
                       .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    return df.withColumn(f'{sum_col}_running_total', F.sum(sum_col).over(window_spec))


def calculate_lag_lead(df: Any, partition_cols: list[str], order_col: str,
                      value_col: str, lag: int = 1, lead: int = 1,
                      engine: str = 'pandas') -> Any:
    """
    Calculate lag and lead values for time series analysis.
    
    Args:
        df: Input DataFrame
        partition_cols: Columns to partition by
        order_col: Column to order by
        value_col: Column to calculate lag/lead for
        lag: Number of periods to lag
        lead: Number of periods to lead
        engine: Engine type ('pandas', 'polars', 'spark')
    
    Returns:
        DataFrame with lag and lead columns added
    """
    if engine == 'pandas':
        return _calculate_lag_lead_pandas(df, partition_cols, order_col, value_col, lag, lead)
    elif engine == 'polars' and POLARS_AVAILABLE:
        return _calculate_lag_lead_polars(df, partition_cols, order_col, value_col, lag, lead)
    elif engine == 'spark' and SPARK_AVAILABLE:
        return _calculate_lag_lead_spark(df, partition_cols, order_col, value_col, lag, lead)
    else:
        raise ValueError(f"Engine '{engine}' not available or not supported")


def _calculate_lag_lead_pandas(df: pd.DataFrame, partition_cols: list[str], order_col: str,
                             value_col: str, lag: int, lead: int) -> pd.DataFrame:
    """Calculate lag and lead using pandas."""
    df = df.copy()
    df = df.sort_values(partition_cols + [order_col])

    df[f'{value_col}_lag_{lag}'] = df.groupby(partition_cols)[value_col].shift(lag)
    df[f'{value_col}_lead_{lead}'] = df.groupby(partition_cols)[value_col].shift(-lead)

    return df


def _calculate_lag_lead_polars(df: pl.DataFrame | pl.LazyFrame, partition_cols: list[str],
                             order_col: str, value_col: str, lag: int, lead: int) -> pl.DataFrame | pl.LazyFrame:
    """Calculate lag and lead using Polars."""
    return df.with_columns([
        pl.col(value_col).shift(lag).over(partition_cols, order_by=order_col).alias(f'{value_col}_lag_{lag}'),
        pl.col(value_col).shift(-lead).over(partition_cols, order_by=order_col).alias(f'{value_col}_lead_{lead}')
    ])


def _calculate_lag_lead_spark(df: SparkDataFrame, partition_cols: list[str], order_col: str,
                            value_col: str, lag: int, lead: int) -> SparkDataFrame:
    """Calculate lag and lead using Spark."""
    window_spec = Window.partitionBy(*partition_cols).orderBy(order_col)

    return df.withColumns({
        f'{value_col}_lag_{lag}': F.lag(value_col, lag).over(window_spec),
        f'{value_col}_lead_{lead}': F.lead(value_col, lead).over(window_spec)
    })


def calculate_rank_percentile(df: Any, partition_cols: list[str], order_col: str,
                            engine: str = 'pandas') -> Any:
    """
    Calculate rank and percentile within partitions.
    
    Args:
        df: Input DataFrame
        partition_cols: Columns to partition by
        order_col: Column to order by for ranking
        engine: Engine type ('pandas', 'polars', 'spark')
    
    Returns:
        DataFrame with rank and percentile columns added
    """
    if engine == 'pandas':
        return _calculate_rank_percentile_pandas(df, partition_cols, order_col)
    elif engine == 'polars' and POLARS_AVAILABLE:
        return _calculate_rank_percentile_polars(df, partition_cols, order_col)
    elif engine == 'spark' and SPARK_AVAILABLE:
        return _calculate_rank_percentile_spark(df, partition_cols, order_col)
    else:
        raise ValueError(f"Engine '{engine}' not available or not supported")


def _calculate_rank_percentile_pandas(df: pd.DataFrame, partition_cols: list[str], order_col: str) -> pd.DataFrame:
    """Calculate rank and percentile using pandas."""
    df = df.copy()

    df['rank'] = df.groupby(partition_cols)[order_col].rank(method='dense', ascending=False)
    df['percentile'] = df.groupby(partition_cols)[order_col].rank(pct=True, ascending=False)

    return df


def _calculate_rank_percentile_polars(df: pl.DataFrame | pl.LazyFrame,
                                    partition_cols: list[str], order_col: str) -> pl.DataFrame | pl.LazyFrame:
    """Calculate rank and percentile using Polars."""
    return df.with_columns([
        pl.col(order_col).rank(method='dense', descending=True).over(partition_cols).alias('rank'),
        pl.col(order_col).rank(method='average', descending=True).over(partition_cols).alias('percentile')
    ])


def _calculate_rank_percentile_spark(df: SparkDataFrame, partition_cols: list[str], order_col: str) -> SparkDataFrame:
    """Calculate rank and percentile using Spark."""
    window_spec = Window.partitionBy(*partition_cols).orderBy(F.desc(order_col))

    return df.withColumns({
        'rank': F.dense_rank().over(window_spec),
        'percentile': F.percent_rank().over(window_spec)
    })


def detect_data_changes(current_df: Any, incoming_df: Any, key_cols: list[str],
                       track_cols: list[str], engine: str = 'pandas') -> Any:
    """
    Detect changes between current and incoming data for SCD2 processing.
    
    Args:
        current_df: Current state DataFrame
        incoming_df: Incoming data DataFrame
        key_cols: Business key columns
        track_cols: Columns to track for changes
        engine: Engine type ('pandas', 'polars', 'spark')
    
    Returns:
        DataFrame with change indicators
    """
    if engine == 'pandas':
        return _detect_data_changes_pandas(current_df, incoming_df, key_cols, track_cols)
    elif engine == 'polars' and POLARS_AVAILABLE:
        return _detect_data_changes_polars(current_df, incoming_df, key_cols, track_cols)
    elif engine == 'spark' and SPARK_AVAILABLE:
        return _detect_data_changes_spark(current_df, incoming_df, key_cols, track_cols)
    else:
        raise ValueError(f"Engine '{engine}' not available or not supported")


def _detect_data_changes_pandas(current_df: pd.DataFrame, incoming_df: pd.DataFrame,
                               key_cols: list[str], track_cols: list[str]) -> pd.DataFrame:
    """Detect changes using pandas."""
    # Generate hash for tracked columns
    current_df = current_df.copy()
    incoming_df = incoming_df.copy()

    current_df['current_hash'] = current_df[track_cols].apply(
        lambda x: hash(tuple(x.astype(str))), axis=1
    )
    incoming_df['incoming_hash'] = incoming_df[track_cols].apply(
        lambda x: hash(tuple(x.astype(str))), axis=1
    )

    # Join on business keys
    comparison = incoming_df.merge(
        current_df[key_cols + ['current_hash']],
        on=key_cols,
        how='left',
        suffixes=('', '_current')
    )

    # Detect changes
    comparison['has_changed'] = (
        comparison['incoming_hash'] != comparison['current_hash']
    ) | comparison['current_hash'].isna()

    comparison['change_type'] = 'no_change'
    comparison.loc[comparison['current_hash'].isna(), 'change_type'] = 'insert'
    comparison.loc[
        (comparison['current_hash'].notna()) & comparison['has_changed'],
        'change_type'
    ] = 'update'

    return comparison


def _detect_data_changes_polars(current_df: pl.DataFrame | pl.LazyFrame,
                               incoming_df: pl.DataFrame | pl.LazyFrame,
                               key_cols: list[str], track_cols: list[str]) -> pl.DataFrame | pl.LazyFrame:
    """Detect changes using Polars."""
    # Generate hash for tracked columns
    current_with_hash = current_df.with_columns([
        pl.concat_str(track_cols, separator='|').hash().alias('current_hash')
    ])

    incoming_with_hash = incoming_df.with_columns([
        pl.concat_str(track_cols, separator='|').hash().alias('incoming_hash')
    ])

    # Join and detect changes
    comparison = incoming_with_hash.join(
        current_with_hash.select(key_cols + ['current_hash']),
        on=key_cols,
        how='left'
    ).with_columns([
        (
            (pl.col('incoming_hash') != pl.col('current_hash')) |
            pl.col('current_hash').is_null()
        ).alias('has_changed')
    ]).with_columns([
        pl.when(pl.col('current_hash').is_null())
        .then(pl.lit('insert'))
        .when(pl.col('has_changed'))
        .then(pl.lit('update'))
        .otherwise(pl.lit('no_change'))
        .alias('change_type')
    ])

    return comparison


def _detect_data_changes_spark(current_df: SparkDataFrame, incoming_df: SparkDataFrame,
                              key_cols: list[str], track_cols: list[str]) -> SparkDataFrame:
    """Detect changes using Spark."""
    # Generate hash for tracked columns
    current_with_hash = current_df.withColumn(
        'current_hash',
        F.hash(*track_cols)
    )

    incoming_with_hash = incoming_df.withColumn(
        'incoming_hash',
        F.hash(*track_cols)
    )

    # Join and detect changes
    comparison = incoming_with_hash.join(
        current_with_hash.select(key_cols + ['current_hash']),
        on=key_cols,
        how='left'
    ).withColumn(
        'has_changed',
        (F.col('incoming_hash') != F.col('current_hash')) | F.col('current_hash').isNull()
    ).withColumn(
        'change_type',
        F.when(F.col('current_hash').isNull(), F.lit('insert'))
        .when(F.col('has_changed'), F.lit('update'))
        .otherwise(F.lit('no_change'))
    )

    return comparison
