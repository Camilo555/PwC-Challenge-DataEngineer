#!/usr/bin/env python3
"""
Dagster startup script for the retail ETL orchestration.

This script starts the Dagster web server and daemon for monitoring
and managing the data pipeline workflows.
"""

import subprocess
import sys
import time
from pathlib import Path

from de_challenge.core.config import settings
from de_challenge.core.logging import get_logger

logger = get_logger(__name__)


def check_dagster_installed():
    """Check if Dagster is installed."""
    try:
        import dagster
        logger.info(f"Dagster version: {dagster.__version__}")
        return True
    except ImportError:
        logger.error("Dagster is not installed. Please install it using: poetry add dagster dagster-webserver")
        return False


def create_dagster_directories():
    """Create necessary directories for Dagster."""
    directories = [
        Path("dagster_home"),
        Path("dagster_home/storage"),
        Path("dagster_home/logs"),
        Path("data/raw/errors"),  # For error file sensor
    ]
    
    for directory in directories:
        directory.mkdir(parents=True, exist_ok=True)
        logger.info(f"Created directory: {directory}")


def start_dagster_webserver(host="127.0.0.1", port=3000):
    """Start the Dagster web server."""
    logger.info(f"Starting Dagster web server on {host}:{port}")
    
    # Set environment variables
    env = {
        "DAGSTER_HOME": str(Path.cwd() / "dagster_home"),
        "PYTHONPATH": str(Path.cwd() / "src"),
    }
    
    try:
        # Start the web server
        cmd = [
            sys.executable, "-m", "dagster", "dev",
            "--host", host,
            "--port", str(port),
            "--module-name", "de_challenge.orchestration.definitions",
        ]
        
        logger.info(f"Running command: {' '.join(cmd)}")
        
        process = subprocess.Popen(
            cmd,
            env={**env, **dict(os.environ) if 'os' in globals() else {}},
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            bufsize=1,
            universal_newlines=True
        )
        
        logger.info("Dagster web server started successfully!")
        logger.info(f"Access the Dagster UI at: http://{host}:{port}")
        logger.info("Press Ctrl+C to stop the server")
        
        # Stream output
        for line in process.stdout:
            print(line.strip())
            
    except KeyboardInterrupt:
        logger.info("Shutting down Dagster web server...")
        process.terminate()
        process.wait()
        logger.info("Dagster web server stopped")
    except Exception as e:
        logger.error(f"Failed to start Dagster web server: {e}")
        raise


def setup_sample_data():
    """Set up sample data for testing the file sensor."""
    import pandas as pd
    
    raw_data_path = Path(settings.raw_data_path)
    raw_data_path.mkdir(parents=True, exist_ok=True)
    
    # Create a small sample dataset if it doesn't exist
    sample_file = raw_data_path / "sample_retail_data.csv"
    
    if not sample_file.exists():
        # Create sample retail data
        sample_data = {
            'InvoiceNo': ['INV001', 'INV002', 'INV003', 'INV004', 'INV005'],
            'StockCode': ['PROD001', 'PROD002', 'PROD003', 'PROD004', 'PROD005'],
            'Description': ['Product 1', 'Product 2', 'Product 3', 'Product 4', 'Product 5'],
            'Quantity': [2, 1, 3, 2, 1],
            'InvoiceDate': pd.date_range('2024-01-01', periods=5, freq='D'),
            'UnitPrice': [10.50, 25.00, 15.75, 8.25, 30.00],
            'CustomerID': ['CUST001', 'CUST002', 'CUST001', 'CUST003', 'CUST002'],
            'Country': ['United Kingdom', 'France', 'Germany', 'Spain', 'Italy']
        }
        
        df = pd.DataFrame(sample_data)
        df.to_csv(sample_file, index=False)
        
        logger.info(f"Created sample data file: {sample_file}")
        logger.info("This file will trigger the file sensor when Dagster starts monitoring")


def main():
    """Main entry point for starting Dagster."""
    import os
    
    logger.info("Starting Dagster orchestration system...")
    
    # Check prerequisites
    if not check_dagster_installed():
        sys.exit(1)
    
    # Create necessary directories
    create_dagster_directories()
    
    # Set up sample data for testing
    setup_sample_data()
    
    # Print information about the pipeline
    logger.info("=" * 60)
    logger.info("Retail ETL Pipeline with Dagster Orchestration")
    logger.info("=" * 60)
    logger.info("Features:")
    logger.info("- File drop sensor for automatic ingestion")
    logger.info("- External API enrichment (currency, country, product)")
    logger.info("- Bronze, Silver, Gold data layers")
    logger.info("- Data quality monitoring")
    logger.info("- Scheduled processing")
    logger.info("=" * 60)
    
    # Start the web server
    host = "127.0.0.1"
    port = 3000
    
    print(f"\n🚀 Starting Dagster UI...")
    print(f"📊 Pipeline: Retail ETL with External API Enrichment")
    print(f"🌐 URL: http://{host}:{port}")
    print(f"📁 Raw data directory: {settings.raw_data_path}")
    print(f"⚡ File sensor: Monitoring for new CSV files")
    print(f"🔄 To trigger pipeline: Drop a CSV file in {settings.raw_data_path}")
    print(f"⏹️  To stop: Press Ctrl+C\n")
    
    start_dagster_webserver(host, port)


if __name__ == "__main__":
    main()