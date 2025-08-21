"""
DataFrame Engine Strategy Pattern
Provides engine-agnostic DataFrame operations for ETL processing.
"""
from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Union
from pathlib import Path
import pandas as pd

try:
    import polars as pl
    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False

try:
    from pyspark.sql import DataFrame as SparkDataFrame, SparkSession
    from pyspark.sql import functions as F
    SPARK_AVAILABLE = True
except ImportError:
    SPARK_AVAILABLE = False


class DataFrameStrategy(ABC):
    """Abstract base class for DataFrame engine strategies."""
    
    @abstractmethod
    def read_parquet(self, path: str) -> Any:
        """Read parquet file into DataFrame."""
        pass
    
    @abstractmethod
    def write_parquet(self, df: Any, path: str, **kwargs) -> None:
        """Write DataFrame to parquet file."""
        pass
    
    @abstractmethod
    def read_csv(self, path: str, **kwargs) -> Any:
        """Read CSV file into DataFrame."""
        pass
    
    @abstractmethod
    def filter(self, df: Any, condition: str) -> Any:
        """Filter DataFrame with condition."""
        pass
    
    @abstractmethod
    def join(self, left: Any, right: Any, on: Union[str, List[str]], how: str = "inner") -> Any:
        """Join two DataFrames."""
        pass
    
    @abstractmethod
    def group_by(self, df: Any, cols: List[str], aggs: Dict[str, str]) -> Any:
        """Group by columns and apply aggregations."""
        pass
    
    @abstractmethod
    def select(self, df: Any, columns: List[str]) -> Any:
        """Select columns from DataFrame."""
        pass
    
    @abstractmethod
    def with_column(self, df: Any, name: str, expression: Any) -> Any:
        """Add new column to DataFrame."""
        pass
    
    @abstractmethod
    def drop_duplicates(self, df: Any, subset: Optional[List[str]] = None) -> Any:
        """Drop duplicate rows."""
        pass
    
    @abstractmethod
    def sort(self, df: Any, by: Union[str, List[str]], ascending: bool = True) -> Any:
        """Sort DataFrame."""
        pass
    
    @abstractmethod
    def count(self, df: Any) -> int:
        """Count rows in DataFrame."""
        pass
    
    @abstractmethod
    def collect(self, df: Any) -> Any:
        """Collect/execute lazy operations."""
        pass


class PandasStrategy(DataFrameStrategy):
    """Pandas implementation for small to medium datasets."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.chunksize = self.config.get('chunksize', 10000)
        self.dtype_backend = self.config.get('dtype_backend', 'pyarrow')
    
    def read_parquet(self, path: str) -> pd.DataFrame:
        """Read parquet file with Pandas."""
        return pd.read_parquet(path, dtype_backend=self.dtype_backend)
    
    def write_parquet(self, df: pd.DataFrame, path: str, **kwargs) -> None:
        """Write DataFrame to parquet."""
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(path, index=False, **kwargs)
    
    def read_csv(self, path: str, **kwargs) -> pd.DataFrame:
        """Read CSV with chunking support."""
        return pd.read_csv(path, dtype_backend=self.dtype_backend, **kwargs)
    
    def filter(self, df: pd.DataFrame, condition: str) -> pd.DataFrame:
        """Filter using query method."""
        return df.query(condition)
    
    def join(self, left: pd.DataFrame, right: pd.DataFrame, on: Union[str, List[str]], how: str = "inner") -> pd.DataFrame:
        """Join DataFrames."""
        return left.merge(right, on=on, how=how)
    
    def group_by(self, df: pd.DataFrame, cols: List[str], aggs: Dict[str, str]) -> pd.DataFrame:
        """Group by and aggregate."""
        return df.groupby(cols).agg(aggs).reset_index()
    
    def select(self, df: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
        """Select columns."""
        return df[columns]
    
    def with_column(self, df: pd.DataFrame, name: str, expression: Any) -> pd.DataFrame:
        """Add column."""
        df_copy = df.copy()
        df_copy[name] = expression
        return df_copy
    
    def drop_duplicates(self, df: pd.DataFrame, subset: Optional[List[str]] = None) -> pd.DataFrame:
        """Drop duplicates."""
        return df.drop_duplicates(subset=subset)
    
    def sort(self, df: pd.DataFrame, by: Union[str, List[str]], ascending: bool = True) -> pd.DataFrame:
        """Sort DataFrame."""
        return df.sort_values(by=by, ascending=ascending)
    
    def count(self, df: pd.DataFrame) -> int:
        """Count rows."""
        return len(df)
    
    def collect(self, df: pd.DataFrame) -> pd.DataFrame:
        """No-op for pandas (already materialized)."""
        return df


class PolarsStrategy(DataFrameStrategy):
    """Polars implementation for high-performance processing."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        if not POLARS_AVAILABLE:
            raise ImportError("Polars is not available. Install with: pip install polars")
        
        self.config = config or {}
        self.lazy_evaluation = self.config.get('lazy_evaluation', True)
        self.streaming = self.config.get('streaming', True)
        self.memory_limit = self.config.get('memory_limit', '8GB')
    
    def read_parquet(self, path: str) -> pl.LazyFrame:
        """Read parquet with lazy evaluation."""
        if self.lazy_evaluation:
            return pl.scan_parquet(path)
        return pl.read_parquet(path)
    
    def write_parquet(self, df: Union[pl.DataFrame, pl.LazyFrame], path: str, **kwargs) -> None:
        """Write to parquet."""
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        if isinstance(df, pl.LazyFrame):
            df = df.collect(streaming=self.streaming)
        df.write_parquet(path, **kwargs)
    
    def read_csv(self, path: str, **kwargs) -> Union[pl.DataFrame, pl.LazyFrame]:
        """Read CSV with Polars."""
        if self.lazy_evaluation:
            return pl.scan_csv(path, **kwargs)
        return pl.read_csv(path, **kwargs)
    
    def filter(self, df: Union[pl.DataFrame, pl.LazyFrame], condition: str) -> Union[pl.DataFrame, pl.LazyFrame]:
        """Filter with Polars expression."""
        # Convert string condition to Polars expression
        # This is a simplified implementation - real one would parse the condition
        return df.filter(pl.expr(condition))
    
    def join(self, left: Union[pl.DataFrame, pl.LazyFrame], right: Union[pl.DataFrame, pl.LazyFrame], 
             on: Union[str, List[str]], how: str = "inner") -> Union[pl.DataFrame, pl.LazyFrame]:
        """Join DataFrames."""
        return left.join(right, on=on, how=how)
    
    def group_by(self, df: Union[pl.DataFrame, pl.LazyFrame], cols: List[str], 
                 aggs: Dict[str, str]) -> Union[pl.DataFrame, pl.LazyFrame]:
        """Group by and aggregate."""
        agg_exprs = []
        for col, agg_func in aggs.items():
            if agg_func == 'sum':
                agg_exprs.append(pl.col(col).sum())
            elif agg_func == 'mean':
                agg_exprs.append(pl.col(col).mean())
            elif agg_func == 'count':
                agg_exprs.append(pl.col(col).count())
            elif agg_func == 'min':
                agg_exprs.append(pl.col(col).min())
            elif agg_func == 'max':
                agg_exprs.append(pl.col(col).max())
        
        return df.group_by(cols).agg(agg_exprs)
    
    def select(self, df: Union[pl.DataFrame, pl.LazyFrame], columns: List[str]) -> Union[pl.DataFrame, pl.LazyFrame]:
        """Select columns."""
        return df.select(columns)
    
    def with_column(self, df: Union[pl.DataFrame, pl.LazyFrame], name: str, 
                    expression: Any) -> Union[pl.DataFrame, pl.LazyFrame]:
        """Add column."""
        return df.with_columns(pl.lit(expression).alias(name))
    
    def drop_duplicates(self, df: Union[pl.DataFrame, pl.LazyFrame], 
                       subset: Optional[List[str]] = None) -> Union[pl.DataFrame, pl.LazyFrame]:
        """Drop duplicates."""
        return df.unique(subset=subset)
    
    def sort(self, df: Union[pl.DataFrame, pl.LazyFrame], by: Union[str, List[str]], 
             ascending: bool = True) -> Union[pl.DataFrame, pl.LazyFrame]:
        """Sort DataFrame."""
        return df.sort(by, descending=not ascending)
    
    def count(self, df: Union[pl.DataFrame, pl.LazyFrame]) -> int:
        """Count rows."""
        if isinstance(df, pl.LazyFrame):
            return df.select(pl.count()).collect().item()
        return len(df)
    
    def collect(self, df: Union[pl.DataFrame, pl.LazyFrame]) -> pl.DataFrame:
        """Collect lazy frame."""
        if isinstance(df, pl.LazyFrame):
            return df.collect(streaming=self.streaming)
        return df


class SparkStrategy(DataFrameStrategy):
    """PySpark implementation for large-scale distributed processing."""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        if not SPARK_AVAILABLE:
            raise ImportError("PySpark is not available. Install with: pip install pyspark")
        
        self.config = config or {}
        self.spark = self._get_spark_session()
    
    def _get_spark_session(self) -> SparkSession:
        """Get or create Spark session with optimized configuration."""
        builder = SparkSession.builder.appName("ETL_Processing")
        
        # Default configurations
        default_config = {
            "spark.sql.adaptive.enabled": "true",
            "spark.sql.adaptive.coalescePartitions.enabled": "true",
            "spark.sql.adaptive.skewJoin.enabled": "true",
            "spark.sql.adaptive.localShuffleReader.enabled": "true",
            "spark.memory.fraction": "0.75",
            "spark.memory.storageFraction": "0.30",
            "spark.sql.shuffle.partitions": "200",
            "spark.default.parallelism": "400",
            "spark.sql.files.maxPartitionBytes": "134217728",
            "spark.sql.files.openCostInBytes": "4194304",
            "spark.sql.execution.arrow.pyspark.enabled": "true"
        }
        
        # Apply configuration
        config = {**default_config, **self.config}
        for key, value in config.items():
            builder = builder.config(key, value)
        
        return builder.getOrCreate()
    
    def read_parquet(self, path: str) -> SparkDataFrame:
        """Read parquet with Spark."""
        return self.spark.read.parquet(path)
    
    def write_parquet(self, df: SparkDataFrame, path: str, **kwargs) -> None:
        """Write to parquet with Spark."""
        mode = kwargs.get('mode', 'overwrite')
        df.write.mode(mode).parquet(path)
    
    def read_csv(self, path: str, **kwargs) -> SparkDataFrame:
        """Read CSV with Spark."""
        return self.spark.read.option("header", "true").option("inferSchema", "true").csv(path)
    
    def filter(self, df: SparkDataFrame, condition: str) -> SparkDataFrame:
        """Filter with SQL condition."""
        return df.filter(condition)
    
    def join(self, left: SparkDataFrame, right: SparkDataFrame, on: Union[str, List[str]], 
             how: str = "inner") -> SparkDataFrame:
        """Join DataFrames with broadcast optimization."""
        # Auto-broadcast smaller DataFrames
        if self._should_broadcast(right):
            from pyspark.sql.functions import broadcast
            right = broadcast(right)
        
        return left.join(right, on=on, how=how)
    
    def group_by(self, df: SparkDataFrame, cols: List[str], aggs: Dict[str, str]) -> SparkDataFrame:
        """Group by and aggregate."""
        agg_exprs = {}
        for col, agg_func in aggs.items():
            agg_exprs[col] = agg_func
        
        return df.groupBy(*cols).agg(agg_exprs)
    
    def select(self, df: SparkDataFrame, columns: List[str]) -> SparkDataFrame:
        """Select columns."""
        return df.select(*columns)
    
    def with_column(self, df: SparkDataFrame, name: str, expression: Any) -> SparkDataFrame:
        """Add column."""
        return df.withColumn(name, F.lit(expression))
    
    def drop_duplicates(self, df: SparkDataFrame, subset: Optional[List[str]] = None) -> SparkDataFrame:
        """Drop duplicates."""
        return df.dropDuplicates(subset)
    
    def sort(self, df: SparkDataFrame, by: Union[str, List[str]], ascending: bool = True) -> SparkDataFrame:
        """Sort DataFrame."""
        if isinstance(by, str):
            by = [by]
        
        if ascending:
            return df.orderBy(*by)
        else:
            return df.orderBy(*[F.desc(col) for col in by])
    
    def count(self, df: SparkDataFrame) -> int:
        """Count rows."""
        return df.count()
    
    def collect(self, df: SparkDataFrame) -> SparkDataFrame:
        """No-op for Spark (lazy by default)."""
        return df
    
    def _should_broadcast(self, df: SparkDataFrame) -> bool:
        """Determine if DataFrame should be broadcast."""
        try:
            # Check if DataFrame is small enough to broadcast
            row_count = df.count()
            return row_count < 10_000_000  # 10M rows threshold
        except Exception:
            return False


class DataFrameEngineFactory:
    """Factory for creating DataFrame engine strategies."""
    
    _strategies = {
        'pandas': PandasStrategy,
        'polars': PolarsStrategy,
        'spark': SparkStrategy
    }
    
    @classmethod
    def create_engine(cls, engine_type: str, config: Optional[Dict[str, Any]] = None) -> DataFrameStrategy:
        """Create DataFrame engine strategy."""
        if engine_type not in cls._strategies:
            available = ', '.join(cls._strategies.keys())
            raise ValueError(f"Unknown engine type: {engine_type}. Available: {available}")
        
        strategy_class = cls._strategies[engine_type]
        return strategy_class(config)
    
    @classmethod
    def get_available_engines(cls) -> List[str]:
        """Get list of available engines."""
        available = []
        
        # Check pandas
        available.append('pandas')
        
        # Check polars
        if POLARS_AVAILABLE:
            available.append('polars')
        
        # Check spark
        if SPARK_AVAILABLE:
            available.append('spark')
        
        return available
    
    @classmethod
    def auto_select_engine(cls, data_size_gb: float, config: Optional[Dict[str, Any]] = None) -> DataFrameStrategy:
        """Auto-select best engine based on data size."""
        if data_size_gb < 1.0:
            # Small data: use pandas
            return cls.create_engine('pandas', config)
        elif data_size_gb < 100.0 and POLARS_AVAILABLE:
            # Medium data: use polars if available
            return cls.create_engine('polars', config)
        elif SPARK_AVAILABLE:
            # Large data: use spark
            return cls.create_engine('spark', config)
        else:
            # Fallback to pandas
            return cls.create_engine('pandas', config)


# Utility function for easy access
def get_dataframe_engine(engine_type: Optional[str] = None, 
                        data_size_gb: Optional[float] = None,
                        config: Optional[Dict[str, Any]] = None) -> DataFrameStrategy:
    """Get DataFrame engine with auto-selection support."""
    factory = DataFrameEngineFactory()
    
    if engine_type:
        return factory.create_engine(engine_type, config)
    elif data_size_gb is not None:
        return factory.auto_select_engine(data_size_gb, config)
    else:
        # Default to pandas
        return factory.create_engine('pandas', config)