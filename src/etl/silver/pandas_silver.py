"""
Windows-Compatible Silver Layer Implementation using Pandas
Provides data cleaning and transformation without Spark/Hadoop dependencies
"""
from __future__ import annotations

import json
import platform
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd
import numpy as np

from core.config import settings
from core.logging import get_logger
from domain.entities.transaction import TransactionLine

logger = get_logger(__name__)

REQUIRED_COLUMNS = [
    "invoice_no",
    "stock_code", 
    "description",
    "quantity",
    "unit_price",
    "invoice_timestamp",
    "customer_id",
    "country",
]


def _read_bronze_data() -> pd.DataFrame:
    """Read data from Bronze layer (Parquet format)."""
    bronze_path = settings.bronze_path / "sales"
    
    if not bronze_path.exists():
        raise FileNotFoundError(f"Bronze dataset not found at {bronze_path}. Run Bronze ingest first.")
    
    # Read all Parquet files from Bronze layer
    parquet_files = list(bronze_path.rglob("*.parquet"))
    if not parquet_files:
        raise FileNotFoundError(f"No Parquet files found in {bronze_path}")
    
    # Read and combine all Parquet files
    dfs = []
    for file in parquet_files:
        try:
            df = pd.read_parquet(file)
            dfs.append(df)
            logger.info(f"Read {len(df)} records from {file.name}")
        except Exception as e:
            logger.warning(f"Failed to read {file}: {e}")
    
    if not dfs:
        raise ValueError("No valid Parquet files could be read")
    
    # Combine all dataframes
    combined_df = pd.concat(dfs, ignore_index=True)
    logger.info(f"Combined {len(combined_df)} total records from {len(dfs)} files")
    
    return combined_df


def _normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names to lowercase."""
    df.columns = [col.lower() for col in df.columns]
    return df


def _ensure_required_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure all required columns exist."""
    for col in REQUIRED_COLUMNS:
        if col not in df.columns:
            df[col] = None
            logger.info(f"Added missing column: {col}")
    return df


def _cast_and_clean_types(df: pd.DataFrame) -> pd.DataFrame:
    """Cast columns to appropriate types and clean data."""
    logger.info("Casting and cleaning data types...")
    
    # Convert timestamps
    if 'invoice_timestamp' in df.columns:
        df['invoice_timestamp'] = pd.to_datetime(df['invoice_timestamp'], errors='coerce')
    
    # Convert numeric columns
    if 'quantity' in df.columns:
        df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').astype('Int64')
    
    if 'unit_price' in df.columns:
        df['unit_price'] = pd.to_numeric(df['unit_price'], errors='coerce')
    
    # Clean string columns
    string_columns = ['invoice_no', 'stock_code', 'description', 'customer_id', 'country']
    for col in string_columns:
        if col in df.columns:
            df[col] = df[col].astype(str).str.strip()
            # Replace 'nan' strings with actual NaN
            df[col] = df[col].replace(['nan', 'None', ''], np.nan)
    
    logger.info(f"Data types after cleaning: {df.dtypes.to_dict()}")
    return df


def _validate_business_rules(df: pd.DataFrame) -> pd.DataFrame:
    """Apply business rules validation."""
    logger.info("Applying business rules validation...")
    
    original_count = len(df)
    
    # Remove records with invalid quantities (negative or zero)
    if 'quantity' in df.columns:
        df = df[df['quantity'] > 0]
        logger.info(f"Removed {original_count - len(df)} records with invalid quantities")
    
    # Remove records with invalid unit prices (negative)
    if 'unit_price' in df.columns:
        df = df[df['unit_price'] >= 0]
        logger.info(f"Removed {original_count - len(df)} records with invalid unit prices")
    
    # Remove records without invoice numbers
    if 'invoice_no' in df.columns:
        df = df[df['invoice_no'].notna()]
        logger.info(f"Removed {original_count - len(df)} records without invoice numbers")
    
    return df


def _add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add derived columns for analysis."""
    logger.info("Adding derived columns...")
    
    # Calculate total amount
    if 'quantity' in df.columns and 'unit_price' in df.columns:
        df['total_amount'] = df['quantity'] * df['unit_price']
    
    # Add date components
    if 'invoice_timestamp' in df.columns:
        df['invoice_date'] = df['invoice_timestamp'].dt.date
        df['invoice_year'] = df['invoice_timestamp'].dt.year
        df['invoice_month'] = df['invoice_timestamp'].dt.month
        df['invoice_quarter'] = df['invoice_timestamp'].dt.quarter
        df['invoice_day_of_week'] = df['invoice_timestamp'].dt.day_name()
    
    # Add processing metadata
    df['processed_timestamp'] = pd.Timestamp.now()
    df['processing_engine'] = 'pandas'
    df['platform'] = platform.system()
    
    return df


def _remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Remove duplicate records based on business key."""
    logger.info("Removing duplicates...")
    
    original_count = len(df)
    
    # Define business key columns for deduplication
    business_key_columns = ['invoice_no', 'stock_code', 'customer_id']
    available_columns = [col for col in business_key_columns if col in df.columns]
    
    if available_columns:
        df = df.drop_duplicates(subset=available_columns, keep='first')
        duplicates_removed = original_count - len(df)
        logger.info(f"Removed {duplicates_removed} duplicate records")
    else:
        logger.warning("No business key columns available for deduplication")
    
    return df


def _generate_quality_report(df: pd.DataFrame) -> Dict[str, Any]:
    """Generate data quality report."""
    report = {
        "total_records": len(df),
        "column_count": len(df.columns),
        "null_counts": df.isnull().sum().to_dict(),
        "data_types": df.dtypes.astype(str).to_dict(),
        "memory_usage_mb": df.memory_usage(deep=True).sum() / 1024 / 1024,
    }
    
    # Add numeric column statistics
    numeric_columns = df.select_dtypes(include=[np.number]).columns
    if len(numeric_columns) > 0:
        report["numeric_statistics"] = df[numeric_columns].describe().to_dict()
    
    return report


def process_silver_layer() -> bool:
    """Main function to process Silver layer data transformation."""
    try:
        logger.info("Starting Silver layer processing with Pandas...")
        
        # Read Bronze data
        df = _read_bronze_data()
        logger.info(f"Loaded {len(df)} records from Bronze layer")
        
        # Data processing pipeline
        df = _normalize_column_names(df)
        df = _ensure_required_columns(df)
        df = _cast_and_clean_types(df)
        df = _validate_business_rules(df)
        df = _add_derived_columns(df)
        df = _remove_duplicates(df)
        
        # Generate quality report
        quality_report = _generate_quality_report(df)
        logger.info(f"Data quality report: {quality_report}")
        
        # Save to Silver layer
        silver_path = settings.silver_path / "sales"
        silver_path.mkdir(parents=True, exist_ok=True)
        
        # Save as Parquet (partitioned by year if possible)
        if 'invoice_year' in df.columns and df['invoice_year'].notna().any():
            # Partition by year
            for year in df['invoice_year'].dropna().unique():
                year_df = df[df['invoice_year'] == year]
                year_path = silver_path / f"year={int(year)}"
                year_path.mkdir(exist_ok=True)
                
                output_file = year_path / f"silver_sales_{int(year)}.parquet"
                year_df.to_parquet(output_file, index=False)
                logger.info(f"Saved {len(year_df)} records for year {int(year)} to {output_file}")
        else:
            # Save as single file
            output_file = silver_path / "silver_sales.parquet"
            df.to_parquet(output_file, index=False)
            logger.info(f"Saved {len(df)} records to {output_file}")
        
        # Save quality report
        report_file = silver_path / "quality_report.json"
        with open(report_file, 'w') as f:
            json.dump(quality_report, f, indent=2, default=str)
        logger.info(f"Quality report saved to {report_file}")
        
        logger.info(f"Silver layer processing completed successfully - {len(df)} records processed")
        return True
        
    except Exception as e:
        logger.error(f"Silver layer processing failed: {e}")
        return False


def main() -> None:
    """Entry point for Silver layer processing."""
    logger.info("Starting Windows-compatible Silver layer ETL...")
    
    success = process_silver_layer()
    if success:
        print("Silver layer processing completed successfully (Pandas-based, Windows-compatible)")
    else:
        print("Silver layer processing failed")
        exit(1)


if __name__ == "__main__":
    main()