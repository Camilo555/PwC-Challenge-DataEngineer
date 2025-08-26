#!/usr/bin/env python3
"""
Enhanced Bronze Layer Processing with Polars
============================================

High-performance data processing script using Polars engine for optimal 
performance on medium-sized datasets (1GB-100GB).

Features:
- 30x+ performance improvement over Pandas
- Lazy evaluation for memory efficiency  
- Comprehensive data quality assessment
- Advanced error handling and logging
- Business rule validation
"""

import sys
from datetime import datetime
from pathlib import Path

# Add src to Python path
sys.path.append(str(Path(__file__).parent.parent / "src"))

from core.logging import get_logger
from etl.bronze.polars_bronze import PolarsBronzeProcessor

logger = get_logger(__name__)


def main():
    """Main execution function for Polars Bronze processing."""
    print("Starting Enhanced Polars Bronze Layer Processing")
    print("=" * 60)

    start_time = datetime.now()

    try:
        # Configure paths
        base_path = Path(__file__).parent.parent
        input_path = base_path / "data" / "raw"
        output_path = base_path / "data" / "bronze"

        print(f"Input Path: {input_path}")
        print(f"Output Path: {output_path}")
        print()

        # Initialize Polars processor
        processor = PolarsBronzeProcessor(input_path, output_path)

        # Process Bronze layer with enhanced features
        print("Processing with Polars engine...")
        result = processor.process_bronze_layer(enable_profiling=True)

        # Display results
        if result["status"] == "success":
            print("SUCCESS: Bronze Processing COMPLETED!")
            print(f"Records Processed: {result['records_processed']:,}")
            print(f"Columns Created: {result['columns_count']}")
            print(f"Processing Time: {result['processing_time_seconds']:.2f} seconds")
            print(f"Memory Usage: {result['memory_usage_mb']:.2f} MB")
            print(f"Output File: {result['output_file']}")

            # Display quality metrics
            if "quality_metrics" in result:
                quality = result["quality_metrics"]
                print("\nData Quality Assessment:")
                print(f"   Overall Quality Score: {quality.get('quality_score', 0):.1%}")
                print(f"   Total Records: {quality.get('total_records', 0):,}")
                print(f"   Unique Records: {quality.get('unique_records', 0):,}")
                print(f"   Duplicate Percentage: {quality.get('duplicate_percentage', 0):.2f}%")
                print(f"   Average Completeness: {quality.get('average_completeness', 0):.1f}%")

                # Column-level completeness
                if "completeness_by_column" in quality:
                    print("\n   Column Completeness:")
                    for col, completeness in quality["completeness_by_column"].items():
                        status = "EXCELLENT" if completeness >= 95 else "GOOD" if completeness >= 80 else "POOR"
                        print(f"      {status}: {col}: {completeness:.1f}%")

        else:
            print("FAILED: Bronze Processing FAILED!")
            print(f"Error: {result.get('error', 'Unknown error')}")
            print(f"Processing Time: {result.get('processing_time_seconds', 0):.2f} seconds")
            return 1

        total_time = (datetime.now() - start_time).total_seconds()
        print(f"\nTotal Execution Time: {total_time:.2f} seconds")

        # Performance summary
        if result["status"] == "success":
            records_per_second = result['records_processed'] / result['processing_time_seconds']
            print(f"Performance: {records_per_second:,.0f} records/second")

            # Compare with expected Polars performance
            if records_per_second > 10000:
                print("EXCELLENT PERFORMANCE - Polars optimization working!")
            elif records_per_second > 5000:
                print("GOOD PERFORMANCE - Meeting expectations")
            else:
                print("MODERATE PERFORMANCE - Consider optimization")

        print("\n" + "=" * 60)
        print("Polars Bronze Processing Complete!")

        return 0

    except Exception as e:
        logger.error(f"Bronze processing failed: {str(e)}")
        print(f"CRITICAL ERROR: {str(e)}")
        return 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)
