"""
Chunked Bronze Layer Implementation
Memory-efficient data ingestion using chunked processing
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

from core.config import settings
from core.logging import get_logger
from etl.framework.chunked_processor import ChunkConfig, chunked_bronze_ingestion

logger = get_logger(__name__)


def ingest_bronze_chunked(chunk_size: int = 10000,
                         max_memory_mb: int = 2048,
                         parallel_chunks: int = 2) -> bool:
    """
    Memory-efficient bronze layer ingestion using chunked processing.
    
    Args:
        chunk_size: Number of rows per chunk
        max_memory_mb: Maximum memory usage in MB
        parallel_chunks: Number of parallel chunk processors
    
    Returns:
        bool: Success status
    """
    try:
        logger.info("Starting chunked Bronze layer ingestion...")

        # Validate paths
        raw_dir = settings.raw_data_path
        bronze_dir = settings.bronze_path / "sales"

        if not raw_dir.exists():
            logger.error(f"Raw data directory not found: {raw_dir}")
            return False

        # Find CSV files
        csv_files = list(raw_dir.glob("*.csv"))
        if not csv_files:
            logger.warning("No CSV files found in raw directory")
            return False

        logger.info(f"Found {len(csv_files)} CSV files to process")

        # Create output directory
        bronze_dir.mkdir(parents=True, exist_ok=True)

        # Process each CSV file with chunked processing
        all_successful = True

        for csv_file in csv_files:
            logger.info(f"Processing file: {csv_file.name}")

            try:
                # Determine output path
                output_file = bronze_dir / f"bronze_{csv_file.stem}.parquet"

                # Process with chunked ingestion
                metrics = chunked_bronze_ingestion(
                    input_path=csv_file,
                    output_path=output_file,
                    chunk_size=chunk_size
                )

                logger.info(f"Processed {csv_file.name}: {metrics.total_rows} rows in {metrics.processing_time_seconds:.2f}s")

            except Exception as e:
                logger.error(f"Failed to process {csv_file.name}: {e}")
                all_successful = False
                continue

        if all_successful:
            logger.info("All files processed successfully with chunked ingestion")
        else:
            logger.warning("Some files failed during chunked ingestion")

        return all_successful

    except Exception as e:
        logger.error(f"Chunked Bronze ingestion failed: {e}")
        return False


def ingest_bronze_adaptive(auto_detect_large_files: bool = True,
                          large_file_threshold_mb: float = 50.0) -> bool:
    """
    Adaptive bronze ingestion that automatically detects large files and uses chunked processing.
    
    Args:
        auto_detect_large_files: Whether to automatically detect large files
        large_file_threshold_mb: Threshold in MB to consider a file large
    
    Returns:
        bool: Success status
    """
    try:
        logger.info("Starting adaptive Bronze layer ingestion...")

        raw_dir = settings.raw_data_path
        bronze_dir = settings.bronze_path / "sales"

        if not raw_dir.exists():
            logger.error(f"Raw data directory not found: {raw_dir}")
            return False

        # Find and categorize CSV files
        csv_files = list(raw_dir.glob("*.csv"))
        if not csv_files:
            logger.warning("No CSV files found in raw directory")
            return False

        small_files = []
        large_files = []

        for csv_file in csv_files:
            file_size_mb = csv_file.stat().st_size / (1024 * 1024)

            if auto_detect_large_files and file_size_mb > large_file_threshold_mb:
                large_files.append((csv_file, file_size_mb))
                logger.info(f"Large file detected: {csv_file.name} ({file_size_mb:.1f}MB)")
            else:
                small_files.append((csv_file, file_size_mb))

        bronze_dir.mkdir(parents=True, exist_ok=True)
        all_successful = True

        # Process small files with standard pandas
        if small_files:
            logger.info(f"Processing {len(small_files)} small files with standard method")

            for csv_file, file_size in small_files:
                try:
                    # Read entire file into memory
                    df = pd.read_csv(csv_file)

                    # Add bronze layer metadata
                    df['ingestion_timestamp'] = pd.Timestamp.now()
                    df['source_file'] = str(csv_file)
                    df['file_size_mb'] = file_size
                    df['processing_method'] = 'standard'

                    # Basic data cleaning
                    df = df.dropna(how='all')

                    # Save as parquet
                    output_file = bronze_dir / f"bronze_{csv_file.stem}.parquet"
                    df.to_parquet(output_file, index=False)

                    logger.info(f"Processed {csv_file.name}: {len(df)} rows")

                except Exception as e:
                    logger.error(f"Failed to process small file {csv_file.name}: {e}")
                    all_successful = False
                    continue

        # Process large files with chunked processing
        if large_files:
            logger.info(f"Processing {len(large_files)} large files with chunked method")

            for csv_file, file_size in large_files:
                try:
                    # Calculate appropriate chunk size based on file size
                    estimated_rows = int(file_size * 1000)  # Rough estimate
                    chunk_size = min(50000, max(1000, estimated_rows // 20))

                    logger.info(f"Using chunk size {chunk_size} for {csv_file.name}")

                    output_file = bronze_dir / f"bronze_{csv_file.stem}.parquet"

                    # Use chunked processing
                    metrics = chunked_bronze_ingestion(
                        input_path=csv_file,
                        output_path=output_file,
                        chunk_size=chunk_size
                    )

                    logger.info(f"Processed large file {csv_file.name}: {metrics.total_rows} rows in {metrics.processing_time_seconds:.2f}s")

                except Exception as e:
                    logger.error(f"Failed to process large file {csv_file.name}: {e}")
                    all_successful = False
                    continue

        if all_successful:
            logger.info("All files processed successfully with adaptive ingestion")
        else:
            logger.warning("Some files failed during adaptive ingestion")

        return all_successful

    except Exception as e:
        logger.error(f"Adaptive Bronze ingestion failed: {e}")
        return False


def monitor_bronze_ingestion_memory() -> dict:
    """
    Monitor memory usage during bronze ingestion.
    
    Returns:
        dict: Memory usage statistics
    """
    try:
        import psutil

        process = psutil.Process()
        memory_info = process.memory_info()

        stats = {
            'memory_mb': memory_info.rss / (1024 * 1024),
            'memory_percent': process.memory_percent(),
            'cpu_percent': process.cpu_percent(),
            'threads': process.num_threads()
        }

        # System memory info
        system_memory = psutil.virtual_memory()
        stats.update({
            'system_memory_total_gb': system_memory.total / (1024**3),
            'system_memory_available_gb': system_memory.available / (1024**3),
            'system_memory_percent': system_memory.percent
        })

        return stats

    except ImportError:
        logger.warning("psutil not available for memory monitoring")
        return {'memory_mb': 0, 'error': 'psutil_not_available'}


def optimize_bronze_chunk_config(file_path: Path) -> ChunkConfig:
    """
    Optimize chunk configuration based on file characteristics.
    
    Args:
        file_path: Path to the file to analyze
    
    Returns:
        ChunkConfig: Optimized configuration
    """
    try:
        # Get file size
        file_size_mb = file_path.stat().st_size / (1024 * 1024)

        # Get system memory
        try:
            import psutil
            available_memory_mb = psutil.virtual_memory().available / (1024 * 1024)
        except ImportError:
            available_memory_mb = 2048  # Default assumption

        # Sample file to estimate row count and complexity
        sample_df = pd.read_csv(file_path, nrows=1000)
        estimated_total_rows = int(file_size_mb * 1000 * len(sample_df) / (sample_df.memory_usage(deep=True).sum() / (1024 * 1024)))

        # Calculate optimal chunk size
        if file_size_mb < 10:
            chunk_size = 5000
            parallel_chunks = 1
        elif file_size_mb < 100:
            chunk_size = 10000
            parallel_chunks = 2
        elif file_size_mb < 500:
            chunk_size = 25000
            parallel_chunks = 3
        else:
            chunk_size = 50000
            parallel_chunks = 4

        # Adjust based on available memory
        max_memory_mb = min(int(available_memory_mb * 0.7), 4096)  # Use 70% of available memory, max 4GB

        config = ChunkConfig(
            chunk_size=chunk_size,
            max_memory_mb=max_memory_mb,
            parallel_chunks=parallel_chunks,
            use_compression=True,
            enable_monitoring=True,
            checkpoint_frequency=10 if file_size_mb > 100 else 0
        )

        logger.info(f"Optimized config for {file_path.name}: chunk_size={chunk_size}, "
                   f"max_memory={max_memory_mb}MB, parallel={parallel_chunks}")

        return config

    except Exception as e:
        logger.warning(f"Failed to optimize config for {file_path}: {e}")
        # Return default config
        return ChunkConfig()


def main() -> None:
    """Entry point for chunked Bronze layer ingestion."""
    logger.info("Starting memory-efficient Bronze layer ETL...")

    # Monitor initial memory state
    initial_memory = monitor_bronze_ingestion_memory()
    logger.info(f"Initial memory usage: {initial_memory.get('memory_mb', 0):.1f}MB")

    # Run adaptive ingestion
    success = ingest_bronze_adaptive()

    # Monitor final memory state
    final_memory = monitor_bronze_ingestion_memory()
    logger.info(f"Final memory usage: {final_memory.get('memory_mb', 0):.1f}MB")

    if success:
        print("Chunked Bronze ingest completed successfully")
    else:
        print("Chunked Bronze ingest failed")
        exit(1)


if __name__ == "__main__":
    main()
