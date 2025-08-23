"""
Chunked ETL Processing Framework
Provides memory-efficient processing by breaking large datasets into manageable chunks
"""
from __future__ import annotations

import gc
import uuid
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterator
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pandas as pd
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql import SparkSession

from core.logging import get_logger

logger = get_logger(__name__)


@dataclass
class ChunkConfig:
    """Configuration for chunked processing."""
    chunk_size: int = 10000
    max_memory_mb: int = 1024
    parallel_chunks: int = 4
    overlap_rows: int = 0
    use_compression: bool = True
    enable_monitoring: bool = True
    checkpoint_frequency: int = 10
    error_tolerance: float = 0.05  # 5% error tolerance


@dataclass
class ProcessingMetrics:
    """Metrics for chunk processing."""
    total_chunks: int = 0
    processed_chunks: int = 0
    failed_chunks: int = 0
    total_rows: int = 0
    processed_rows: int = 0
    memory_peak_mb: float = 0.0
    processing_time_seconds: float = 0.0
    error_rate: float = 0.0


class ChunkProcessor(ABC):
    """Abstract base class for chunk processors."""

    def __init__(self, config: ChunkConfig):
        self.config = config
        self.metrics = ProcessingMetrics()
        self.checkpoint_data: dict[str, Any] = {}

    @abstractmethod
    def process_chunk(self, chunk: pd.DataFrame | SparkDataFrame, chunk_id: str) -> pd.DataFrame | SparkDataFrame:
        """Process a single chunk of data."""
        pass

    @abstractmethod
    def combine_chunks(self, chunk_results: list[pd.DataFrame | SparkDataFrame]) -> pd.DataFrame | SparkDataFrame:
        """Combine processed chunks into final result."""
        pass

    def validate_chunk(self, chunk: pd.DataFrame | SparkDataFrame) -> bool:
        """Validate chunk before processing."""
        if isinstance(chunk, pd.DataFrame):
            return not chunk.empty and len(chunk) > 0
        elif hasattr(chunk, 'count'):  # Spark DataFrame
            return chunk.count() > 0
        return False

    def handle_chunk_error(self, chunk_id: str, error: Exception) -> bool:
        """Handle error in chunk processing. Return True to continue, False to abort."""
        logger.error(f"Error processing chunk {chunk_id}: {error}")
        self.metrics.failed_chunks += 1

        error_rate = self.metrics.failed_chunks / max(1, self.metrics.total_chunks)
        if error_rate > self.config.error_tolerance:
            logger.error(f"Error rate {error_rate:.2%} exceeds tolerance {self.config.error_tolerance:.2%}")
            return False

        return True

    def create_checkpoint(self, chunk_id: str, data: Any) -> None:
        """Create checkpoint for recovery."""
        if self.config.checkpoint_frequency > 0:
            self.checkpoint_data[chunk_id] = data

    def load_checkpoint(self, chunk_id: str) -> Any | None:
        """Load checkpoint data."""
        return self.checkpoint_data.get(chunk_id)


class PandasChunkProcessor(ChunkProcessor):
    """Pandas-based chunk processor."""

    def __init__(self, config: ChunkConfig, process_function: Callable[[pd.DataFrame], pd.DataFrame]):
        super().__init__(config)
        self.process_function = process_function

    def process_chunk(self, chunk: pd.DataFrame, chunk_id: str) -> pd.DataFrame:
        """Process a pandas DataFrame chunk."""
        if not self.validate_chunk(chunk):
            raise ValueError(f"Invalid chunk {chunk_id}")

        try:
            # Apply custom processing function
            result = self.process_function(chunk)

            # Add chunk metadata
            result = result.copy()
            result['_chunk_id'] = chunk_id
            result['_processed_timestamp'] = pd.Timestamp.now()

            self.metrics.processed_rows += len(result)
            logger.debug(f"Processed chunk {chunk_id}: {len(result)} rows")

            return result

        except Exception as e:
            if not self.handle_chunk_error(chunk_id, e):
                raise
            # Return empty DataFrame on recoverable error
            return pd.DataFrame()

    def combine_chunks(self, chunk_results: list[pd.DataFrame]) -> pd.DataFrame:
        """Combine pandas DataFrame chunks."""
        valid_chunks = [chunk for chunk in chunk_results if not chunk.empty]

        if not valid_chunks:
            logger.warning("No valid chunks to combine")
            return pd.DataFrame()

        logger.info(f"Combining {len(valid_chunks)} chunks")
        combined = pd.concat(valid_chunks, ignore_index=True)

        # Remove chunk metadata columns
        metadata_cols = [col for col in combined.columns if col.startswith('_chunk_')]
        combined = combined.drop(columns=metadata_cols, errors='ignore')

        return combined


class SparkChunkProcessor(ChunkProcessor):
    """Spark-based chunk processor."""

    def __init__(self, config: ChunkConfig, spark_session: SparkSession,
                 process_function: Callable[[SparkDataFrame], SparkDataFrame]):
        super().__init__(config)
        self.spark = spark_session
        self.process_function = process_function

    def process_chunk(self, chunk: SparkDataFrame, chunk_id: str) -> SparkDataFrame:
        """Process a Spark DataFrame chunk."""
        if not self.validate_chunk(chunk):
            raise ValueError(f"Invalid chunk {chunk_id}")

        try:
            # Apply custom processing function
            result = self.process_function(chunk)

            # Add chunk metadata
            from pyspark.sql import functions as F
            result = result.withColumn('_chunk_id', F.lit(chunk_id))
            result = result.withColumn('_processed_timestamp', F.current_timestamp())

            row_count = result.count()
            self.metrics.processed_rows += row_count
            logger.debug(f"Processed chunk {chunk_id}: {row_count} rows")

            return result

        except Exception as e:
            if not self.handle_chunk_error(chunk_id, e):
                raise
            # Return empty DataFrame on recoverable error
            return self.spark.createDataFrame([], chunk.schema)

    def combine_chunks(self, chunk_results: list[SparkDataFrame]) -> SparkDataFrame:
        """Combine Spark DataFrame chunks."""
        valid_chunks = [chunk for chunk in chunk_results if chunk.count() > 0]

        if not valid_chunks:
            logger.warning("No valid chunks to combine")
            return self.spark.createDataFrame([], chunk_results[0].schema if chunk_results else None)

        logger.info(f"Combining {len(valid_chunks)} chunks")
        combined = valid_chunks[0]
        for chunk in valid_chunks[1:]:
            combined = combined.unionByName(chunk, allowMissingColumns=True)

        # Remove chunk metadata columns
        metadata_cols = [col for col in combined.columns if col.startswith('_chunk_')]
        for col in metadata_cols:
            combined = combined.drop(col)

        return combined


class ChunkedDataReader:
    """Reads data in chunks from various sources."""

    @staticmethod
    def read_csv_chunks(file_path: str | Path, config: ChunkConfig) -> Iterator[pd.DataFrame]:
        """Read CSV file in chunks."""
        logger.info(f"Reading CSV in chunks: {file_path}")

        chunk_reader = pd.read_csv(
            file_path,
            chunksize=config.chunk_size,
            low_memory=False
        )

        chunk_id = 0
        for chunk in chunk_reader:
            chunk_id += 1
            logger.debug(f"Read chunk {chunk_id}: {len(chunk)} rows")
            yield chunk

    @staticmethod
    def read_parquet_chunks(file_path: str | Path, config: ChunkConfig) -> Iterator[pd.DataFrame]:
        """Read Parquet file in chunks."""
        logger.info(f"Reading Parquet in chunks: {file_path}")

        # Read parquet file info first
        import pyarrow.parquet as pq
        parquet_file = pq.ParquetFile(file_path)

        # Process row groups as chunks
        for i in range(parquet_file.num_row_groups):
            chunk = parquet_file.read_row_group(i).to_pandas()
            logger.debug(f"Read chunk {i+1}: {len(chunk)} rows")
            yield chunk

    @staticmethod
    def read_spark_chunks(df: SparkDataFrame, config: ChunkConfig) -> Iterator[SparkDataFrame]:
        """Split Spark DataFrame into chunks."""
        logger.info("Creating Spark DataFrame chunks")

        total_rows = df.count()
        num_chunks = (total_rows + config.chunk_size - 1) // config.chunk_size

        logger.info(f"Splitting {total_rows} rows into {num_chunks} chunks")

        # Add row numbers and split
        from pyspark.sql import functions as F
        from pyspark.sql.window import Window

        window_spec = Window.orderBy(F.monotonically_increasing_id())
        df_with_row_num = df.withColumn("_row_num", F.row_number().over(window_spec))

        for chunk_id in range(num_chunks):
            start_row = chunk_id * config.chunk_size + 1
            end_row = (chunk_id + 1) * config.chunk_size

            chunk = df_with_row_num.filter(
                (F.col("_row_num") >= start_row) & (F.col("_row_num") <= end_row)
            ).drop("_row_num")

            if chunk.count() > 0:
                yield chunk


class ChunkedETLEngine:
    """Main engine for chunked ETL processing."""

    def __init__(self, config: ChunkConfig):
        self.config = config
        self.total_metrics = ProcessingMetrics()

    def process_pandas_etl(self,
                          input_path: str | Path,
                          output_path: str | Path,
                          process_function: Callable[[pd.DataFrame], pd.DataFrame],
                          file_format: str = 'csv') -> ProcessingMetrics:
        """Process ETL with pandas in chunks."""
        import time
        start_time = time.time()

        logger.info(f"Starting chunked pandas ETL: {input_path} -> {output_path}")

        # Choose appropriate reader
        if file_format.lower() == 'csv':
            chunk_reader = ChunkedDataReader.read_csv_chunks(input_path, self.config)
        elif file_format.lower() == 'parquet':
            chunk_reader = ChunkedDataReader.read_parquet_chunks(input_path, self.config)
        else:
            raise ValueError(f"Unsupported file format: {file_format}")

        processor = PandasChunkProcessor(self.config, process_function)

        # Process chunks
        if self.config.parallel_chunks > 1:
            chunk_results = self._process_chunks_parallel_pandas(chunk_reader, processor)
        else:
            chunk_results = self._process_chunks_sequential_pandas(chunk_reader, processor)

        # Combine results
        final_result = processor.combine_chunks(chunk_results)

        # Save result
        output_path = Path(output_path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        if output_path.suffix.lower() == '.csv':
            final_result.to_csv(output_path, index=False)
        elif output_path.suffix.lower() == '.parquet':
            final_result.to_parquet(output_path, index=False)
        else:
            # Default to parquet
            final_result.to_parquet(output_path.with_suffix('.parquet'), index=False)

        # Update metrics
        self.total_metrics.processing_time_seconds = time.time() - start_time
        self.total_metrics.total_rows = len(final_result)

        logger.info(f"Chunked ETL completed: {len(final_result)} rows in {self.total_metrics.processing_time_seconds:.2f}s")

        return self.total_metrics

    def process_spark_etl(self,
                         spark_df: SparkDataFrame,
                         output_path: str | Path,
                         process_function: Callable[[SparkDataFrame], SparkDataFrame]) -> ProcessingMetrics:
        """Process ETL with Spark in chunks."""
        import time
        start_time = time.time()

        logger.info(f"Starting chunked Spark ETL -> {output_path}")

        chunk_reader = ChunkedDataReader.read_spark_chunks(spark_df, self.config)
        processor = SparkChunkProcessor(self.config, spark_df.sql_ctx.sparkSession, process_function)

        # Process chunks sequentially for Spark (parallel processing handled by Spark internally)
        chunk_results = self._process_chunks_sequential_spark(chunk_reader, processor)

        # Combine results
        final_result = processor.combine_chunks(chunk_results)

        # Save result
        output_path = Path(output_path)
        final_result.write.mode("overwrite").parquet(str(output_path))

        # Update metrics
        self.total_metrics.processing_time_seconds = time.time() - start_time
        self.total_metrics.total_rows = final_result.count()

        logger.info(f"Chunked Spark ETL completed: {self.total_metrics.total_rows} rows in {self.total_metrics.processing_time_seconds:.2f}s")

        return self.total_metrics

    def _process_chunks_sequential_pandas(self, chunk_reader: Iterator[pd.DataFrame],
                                        processor: PandasChunkProcessor) -> list[pd.DataFrame]:
        """Process chunks sequentially with pandas."""
        chunk_results = []

        for chunk_id, chunk in enumerate(chunk_reader):
            try:
                self._monitor_memory()

                result = processor.process_chunk(chunk, f"chunk_{chunk_id}")
                chunk_results.append(result)

                processor.metrics.processed_chunks += 1

                # Force garbage collection periodically
                if chunk_id % 10 == 0:
                    gc.collect()

            except Exception as e:
                logger.error(f"Failed to process chunk {chunk_id}: {e}")
                if not processor.handle_chunk_error(f"chunk_{chunk_id}", e):
                    raise

        processor.metrics.total_chunks = len(chunk_results)
        return chunk_results

    def _process_chunks_parallel_pandas(self, chunk_reader: Iterator[pd.DataFrame],
                                      processor: PandasChunkProcessor) -> list[pd.DataFrame]:
        """Process chunks in parallel with pandas."""
        chunk_results = []

        with ThreadPoolExecutor(max_workers=self.config.parallel_chunks) as executor:
            # Submit chunks for processing
            future_to_chunk = {}

            for chunk_id, chunk in enumerate(chunk_reader):
                future = executor.submit(processor.process_chunk, chunk, f"chunk_{chunk_id}")
                future_to_chunk[future] = chunk_id

            # Collect results
            for future in as_completed(future_to_chunk):
                chunk_id = future_to_chunk[future]
                try:
                    result = future.result()
                    chunk_results.append(result)
                    processor.metrics.processed_chunks += 1

                except Exception as e:
                    logger.error(f"Failed to process chunk {chunk_id}: {e}")
                    if not processor.handle_chunk_error(f"chunk_{chunk_id}", e):
                        raise

        processor.metrics.total_chunks = len(chunk_results)
        return chunk_results

    def _process_chunks_sequential_spark(self, chunk_reader: Iterator[SparkDataFrame],
                                       processor: SparkChunkProcessor) -> list[SparkDataFrame]:
        """Process chunks sequentially with Spark."""
        chunk_results = []

        for chunk_id, chunk in enumerate(chunk_reader):
            try:
                result = processor.process_chunk(chunk, f"chunk_{chunk_id}")
                chunk_results.append(result)

                processor.metrics.processed_chunks += 1

            except Exception as e:
                logger.error(f"Failed to process chunk {chunk_id}: {e}")
                if not processor.handle_chunk_error(f"chunk_{chunk_id}", e):
                    raise

        processor.metrics.total_chunks = len(chunk_results)
        return chunk_results

    def _monitor_memory(self) -> None:
        """Monitor memory usage during processing."""
        if not self.config.enable_monitoring:
            return

        try:
            import psutil
            process = psutil.Process()
            memory_mb = process.memory_info().rss / 1024 / 1024

            self.total_metrics.memory_peak_mb = max(self.total_metrics.memory_peak_mb, memory_mb)

            if memory_mb > self.config.max_memory_mb:
                logger.warning(f"Memory usage {memory_mb:.1f}MB exceeds limit {self.config.max_memory_mb}MB")
                gc.collect()

        except ImportError:
            logger.debug("psutil not available for memory monitoring")


# Convenience functions for common ETL patterns
def chunked_bronze_ingestion(input_path: str | Path, output_path: str | Path,
                           chunk_size: int = 10000) -> ProcessingMetrics:
    """Chunked bronze layer data ingestion."""

    def bronze_transform(df: pd.DataFrame) -> pd.DataFrame:
        """Basic bronze transformation."""
        # Add metadata columns
        df = df.copy()
        df['ingestion_timestamp'] = pd.Timestamp.now()
        df['ingestion_id'] = str(uuid.uuid4())
        df['source_file'] = str(input_path)

        # Basic data cleaning
        df = df.dropna(how='all')  # Remove completely empty rows

        return df

    config = ChunkConfig(chunk_size=chunk_size, parallel_chunks=2)
    engine = ChunkedETLEngine(config)

    return engine.process_pandas_etl(input_path, output_path, bronze_transform, 'csv')


def chunked_silver_processing(input_path: str | Path, output_path: str | Path,
                            chunk_size: int = 10000) -> ProcessingMetrics:
    """Chunked silver layer data processing."""

    def silver_transform(df: pd.DataFrame) -> pd.DataFrame:
        """Advanced silver transformation."""
        df = df.copy()

        # Data quality improvements
        # Remove duplicates within chunk
        df = df.drop_duplicates()

        # Add data quality flags
        df['data_quality_score'] = 1.0

        # Type conversions and validations
        if 'quantity' in df.columns:
            df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce')
            df.loc[df['quantity'] <= 0, 'data_quality_score'] *= 0.8

        if 'unit_price' in df.columns:
            df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')
            df.loc[df['unit_price'] < 0, 'data_quality_score'] *= 0.8

        # Calculate derived fields
        if 'quantity' in df.columns and 'unit_price' in df.columns:
            df['total_amount'] = df['quantity'] * df['unit_price']

        df['processed_timestamp'] = pd.Timestamp.now()

        return df

    config = ChunkConfig(chunk_size=chunk_size, parallel_chunks=4)
    engine = ChunkedETLEngine(config)

    return engine.process_pandas_etl(input_path, output_path, silver_transform, 'parquet')
