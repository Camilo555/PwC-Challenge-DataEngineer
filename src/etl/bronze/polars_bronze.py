import logging
from pathlib import Path
from typing import Dict, Any, Optional
import polars as pl
import pandas as pd
from datetime import datetime

from core.logging import get_logger

logger = get_logger(__name__)


class PolarsBronzeProcessor:
    """
    High-performance Polars-based Bronze layer processor.
    Optimized for medium datasets (1GB-100GB) with lazy evaluation.
    """
    
    def __init__(self, input_path: Path, output_path: Path):
        self.input_path = Path(input_path)
        self.output_path = Path(output_path)
        self.output_path.mkdir(parents=True, exist_ok=True)
        
    def process_bronze_layer(self, enable_profiling: bool = True) -> Dict[str, Any]:
        """
        Process raw data into Bronze layer using Polars lazy evaluation.
        
        Returns comprehensive processing metrics and quality assessment.
        """
        logger.info("Starting Polars Bronze layer processing")
        start_time = datetime.now()
        
        try:
            # Use lazy evaluation for optimal performance
            df_lazy = self._load_data_lazily()
            
            # Apply bronze transformations
            df_processed = self._apply_bronze_transformations(df_lazy)
            
            # Collect results (triggers execution)
            df_result = df_processed.collect()
            
            # Data profiling and quality assessment
            quality_metrics = self._assess_data_quality(df_result) if enable_profiling else {}
            
            # Save to Bronze layer
            output_file = self.output_path / "bronze_retail_data.parquet"
            df_result.write_parquet(output_file, compression="snappy")
            
            processing_time = (datetime.now() - start_time).total_seconds()
            
            result = {
                "status": "success",
                "records_processed": len(df_result),
                "columns_count": len(df_result.columns),
                "processing_time_seconds": processing_time,
                "output_file": str(output_file),
                "quality_metrics": quality_metrics,
                "memory_usage_mb": df_result.estimated_size("mb"),
            }
            
            logger.info(f"Bronze processing completed: {result['records_processed']:,} records in {processing_time:.2f}s")
            return result
            
        except Exception as e:
            logger.error(f"Bronze processing failed: {str(e)}")
            return {
                "status": "failed",
                "error": str(e),
                "processing_time_seconds": (datetime.now() - start_time).total_seconds()
            }
    
    def _load_data_lazily(self) -> pl.LazyFrame:
        """Load data using Polars lazy evaluation for optimal performance."""
        file_path = self._find_input_file()
        
        if file_path.suffix.lower() == '.csv':
            return pl.scan_csv(
                file_path,
                infer_schema_length=10000,
                try_parse_dates=True,
                encoding="utf8-lossy"  # Handle encoding issues gracefully
            )
        elif file_path.suffix.lower() == '.parquet':
            return pl.scan_parquet(file_path)
        elif file_path.suffix.lower() in ['.xlsx', '.xls']:
            # For Excel files, load with pandas first then convert
            df_pandas = pd.read_excel(file_path, engine='openpyxl')
            return pl.from_pandas(df_pandas).lazy()
        else:
            raise ValueError(f"Unsupported file format: {file_path.suffix}")
    
    def _find_input_file(self) -> Path:
        """Find the input data file."""
        if self.input_path.is_file():
            return self.input_path
        
        # Look for common data files
        for pattern in ["*.csv", "*.parquet", "*.xlsx", "*.xls"]:
            files = list(self.input_path.glob(pattern))
            if files:
                return files[0]  # Return first matching file
        
        raise FileNotFoundError(f"No supported data files found in {self.input_path}")
    
    def _apply_bronze_transformations(self, df_lazy: pl.LazyFrame) -> pl.LazyFrame:
        """
        Apply Bronze layer transformations using Polars expressions.
        Focus on data standardization and basic cleaning.
        """
        # Collect to see actual columns first
        df_sample = df_lazy.limit(1).collect()
        actual_columns = df_sample.columns
        logger.info(f"Actual columns in data: {actual_columns}")
        
        # Get column mapping and filter only existing ones
        column_mapping = self._get_column_mapping()
        valid_mapping = {k: v for k, v in column_mapping.items() if k in actual_columns}
        logger.info(f"Valid column mapping: {valid_mapping}")
        
        # Start with base lazy frame
        result_df = df_lazy
        
        # Apply column renaming if we have valid mappings
        if valid_mapping:
            result_df = result_df.rename(valid_mapping)
        
        # Get final column names after renaming
        final_columns = result_df.limit(1).collect().columns
        logger.info(f"Final columns after renaming: {final_columns}")
        
        # Add metadata columns
        result_df = result_df.with_columns([
            pl.lit(datetime.now().isoformat()).alias("ingestion_timestamp"),
            pl.lit("bronze").alias("data_layer"),
            pl.arange(0, pl.len()).alias("row_id")
        ])
        
        # Handle missing values and type casting safely
        if "customer_id" in final_columns:
            result_df = result_df.with_columns([
                pl.when(pl.col("customer_id").is_null())
                .then(pl.lit("UNKNOWN"))
                .otherwise(pl.col("customer_id").cast(pl.Utf8))
                .alias("customer_id")
            ])
        
        if "country" in final_columns:
            result_df = result_df.with_columns([
                pl.when(pl.col("country").is_null())
                .then(pl.lit("Unknown"))
                .otherwise(pl.col("country"))
                .alias("country")
            ])
        
        # Safe numeric casting
        if "quantity" in final_columns:
            result_df = result_df.with_columns([
                pl.col("quantity").cast(pl.Int64, strict=False)
            ])
        
        if "unit_price" in final_columns:
            result_df = result_df.with_columns([
                pl.col("unit_price").cast(pl.Float64, strict=False)
            ])
        
        # Calculate derived fields
        if "quantity" in final_columns and "unit_price" in final_columns:
            result_df = result_df.with_columns([
                (pl.col("quantity") * pl.col("unit_price")).alias("total_amount")
            ])
        
        # Apply filters safely
        filters = []
        if "quantity" in final_columns:
            filters.extend([
                pl.col("quantity").is_not_null(),
                pl.col("quantity") > 0
            ])
        if "unit_price" in final_columns:
            filters.extend([
                pl.col("unit_price").is_not_null(),
                pl.col("unit_price") > 0
            ])
        if "invoice_no" in final_columns:
            filters.append(~pl.col("invoice_no").str.starts_with("C"))
        
        if filters:
            # Combine all filters with AND
            combined_filter = filters[0]
            for f in filters[1:]:
                combined_filter = combined_filter & f
            result_df = result_df.filter(combined_filter)
        
        return result_df
    
    def _get_column_mapping(self) -> Dict[str, str]:
        """Map common column variations to standard names."""
        return {
            # Capital case variations
            "InvoiceNo": "invoice_no",
            "StockCode": "stock_code", 
            "Description": "description",
            "Quantity": "quantity",
            "InvoiceDate": "invoice_date",
            "InvoiceTimestamp": "invoice_date",
            "UnitPrice": "unit_price",
            "CustomerID": "customer_id",
            "Country": "country",
            # Lowercase variations (already correct)
            "invoice_no": "invoice_no",
            "stock_code": "stock_code",
            "description": "description", 
            "quantity": "quantity",
            "invoice_timestamp": "invoice_date",
            "unit_price": "unit_price",
            "customer_id": "customer_id",
            "country": "country"
        }
    
    def _assess_data_quality(self, df: pl.DataFrame) -> Dict[str, Any]:
        """
        Comprehensive data quality assessment using Polars expressions.
        """
        total_records = len(df)
        
        # Calculate completeness for each column
        completeness = {}
        for col in df.columns:
            non_null_count = df.filter(pl.col(col).is_not_null()).shape[0]
            completeness[col] = (non_null_count / total_records) * 100 if total_records > 0 else 0
        
        # Calculate duplicate percentage
        unique_records = df.unique().shape[0]
        duplicate_percentage = ((total_records - unique_records) / total_records * 100) if total_records > 0 else 0
        
        # Numeric column statistics
        numeric_stats = {}
        numeric_columns = ['quantity', 'unit_price', 'total_amount']
        for col in numeric_columns:
            if col in df.columns:
                col_stats = df.select(pl.col(col)).describe()
                numeric_stats[col] = col_stats.to_dict(as_series=False)
        
        # Overall quality score (weighted average)
        avg_completeness = sum(completeness.values()) / len(completeness) if completeness else 0
        quality_score = (avg_completeness * 0.6 + (100 - duplicate_percentage) * 0.4) / 100
        
        return {
            "total_records": total_records,
            "unique_records": unique_records,
            "duplicate_percentage": round(duplicate_percentage, 2),
            "completeness_by_column": {k: round(v, 2) for k, v in completeness.items()},
            "average_completeness": round(avg_completeness, 2),
            "numeric_statistics": numeric_stats,
            "quality_score": round(quality_score, 4),
            "assessment_timestamp": datetime.now().isoformat()
        }