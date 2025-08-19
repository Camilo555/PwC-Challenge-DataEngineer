#!/usr/bin/env python3
"""
Airflow startup script for the retail ETL orchestration.

This script initializes and starts Apache Airflow for managing
the data pipeline workflows as an alternative to Dagster.
"""

import os
import subprocess
import sys
import time
from pathlib import Path

sys.path.append(str(Path.cwd() / "src"))

from core.config import settings
from core.logging import get_logger

logger = get_logger(__name__)


def check_airflow_installed():
    """Check if Airflow is installed."""
    try:
        import airflow
        logger.info(f"Airflow version: {airflow.__version__}")
        return True
    except ImportError:
        logger.error("Airflow is not installed. Please install it using: poetry add apache-airflow")
        return False


def setup_airflow_environment():
    """Set up Airflow environment variables and directories."""
    # Create Airflow home directory
    airflow_home = Path.cwd() / "airflow_home"
    airflow_home.mkdir(exist_ok=True)
    
    # Set environment variables
    env_vars = {
        "AIRFLOW_HOME": str(airflow_home),
        "AIRFLOW__CORE__DAGS_FOLDER": str(Path.cwd() / "src" / "airflow_dags"),
        "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
        "AIRFLOW__CORE__EXECUTOR": "LocalExecutor",
        "AIRFLOW__DATABASE__SQL_ALCHEMY_CONN": f"sqlite:///{airflow_home}/airflow.db",
        "AIRFLOW__WEBSERVER__WEB_SERVER_PORT": "8080",
        "AIRFLOW__WEBSERVER__WEB_SERVER_HOST": "127.0.0.1",
        "AIRFLOW__CORE__ENABLE_XCOM_PICKLING": "True",
        "PYTHONPATH": str(Path.cwd() / "src"),
    }
    
    for key, value in env_vars.items():
        os.environ[key] = value
        logger.info(f"Set {key}={value}")
    
    return airflow_home


def initialize_airflow_db(airflow_home):
    """Initialize Airflow database."""
    logger.info("Initializing Airflow database...")
    
    try:
        result = subprocess.run(
            [sys.executable, "-m", "airflow", "db", "init"],
            cwd=airflow_home.parent,
            capture_output=True,
            text=True,
            timeout=120
        )
        
        if result.returncode == 0:
            logger.info("Airflow database initialized successfully")
        else:
            logger.error(f"Database initialization failed: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        logger.error("Database initialization timed out")
        return False
    except Exception as e:
        logger.error(f"Database initialization error: {e}")
        return False
    
    return True


def create_airflow_user():
    """Create admin user for Airflow."""
    logger.info("Creating Airflow admin user...")
    
    try:
        result = subprocess.run([
            sys.executable, "-m", "airflow", "users", "create",
            "--username", "admin",
            "--firstname", "Admin",
            "--lastname", "User", 
            "--role", "Admin",
            "--email", "admin@example.com",
            "--password", "admin123"
        ], capture_output=True, text=True, timeout=60)
        
        if result.returncode == 0:
            logger.info("Admin user created successfully")
            logger.info("Login credentials: admin / admin123")
        else:
            if "already exists" in result.stderr:
                logger.info("Admin user already exists")
            else:
                logger.warning(f"User creation warning: {result.stderr}")
                
    except Exception as e:
        logger.warning(f"User creation failed (non-critical): {e}")


def start_airflow_scheduler():
    """Start Airflow scheduler in background."""
    logger.info("Starting Airflow scheduler...")
    
    try:
        scheduler_process = subprocess.Popen([
            sys.executable, "-m", "airflow", "scheduler"
        ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        # Give scheduler time to start
        time.sleep(5)
        
        if scheduler_process.poll() is None:
            logger.info("Airflow scheduler started successfully")
            return scheduler_process
        else:
            logger.error("Scheduler failed to start")
            return None
            
    except Exception as e:
        logger.error(f"Failed to start scheduler: {e}")
        return None


def start_airflow_webserver():
    """Start Airflow web server."""
    logger.info("Starting Airflow web server...")
    
    try:
        webserver_process = subprocess.Popen([
            sys.executable, "-m", "airflow", "webserver",
            "--port", "8080",
            "--hostname", "127.0.0.1"
        ], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        
        logger.info("Airflow web server started")
        logger.info("Access Airflow UI at: http://127.0.0.1:8080")
        logger.info("Login: admin / admin123")
        
        return webserver_process
        
    except Exception as e:
        logger.error(f"Failed to start web server: {e}")
        return None


def create_sample_data():
    """Create sample data for testing the pipeline."""
    import pandas as pd
    
    raw_data_path = Path(settings.raw_data_path)
    raw_data_path.mkdir(parents=True, exist_ok=True)
    
    sample_file = raw_data_path / "airflow_sample_data.csv"
    
    if not sample_file.exists():
        # Create sample retail data
        sample_data = {
            'InvoiceNo': ['INV001', 'INV002', 'INV003', 'INV004', 'INV005'],
            'StockCode': ['PROD001', 'PROD002', 'PROD003', 'PROD004', 'PROD005'],
            'Description': ['Product A', 'Product B', 'Product C', 'Product D', 'Product E'],
            'Quantity': [2, 1, 3, 2, 1],
            'InvoiceDate': pd.date_range('2024-01-01', periods=5, freq='D'),
            'UnitPrice': [10.50, 25.00, 15.75, 8.25, 30.00],
            'CustomerID': ['CUST001', 'CUST002', 'CUST001', 'CUST003', 'CUST002'],
            'Country': ['United Kingdom', 'France', 'Germany', 'Spain', 'Italy']
        }
        
        df = pd.DataFrame(sample_data)
        df.to_csv(sample_file, index=False)
        
        logger.info(f"Created sample data file: {sample_file}")


def main():
    """Main entry point for starting Airflow."""
    logger.info("Starting Airflow orchestration system...")
    
    # Check prerequisites
    if not check_airflow_installed():
        logger.error("Please install Airflow: poetry add apache-airflow[postgres,redis,celery]")
        sys.exit(1)
    
    # Setup environment
    airflow_home = setup_airflow_environment()
    
    # Initialize database
    if not initialize_airflow_db(airflow_home):
        logger.error("Failed to initialize Airflow database")
        sys.exit(1)
    
    # Create admin user
    create_airflow_user()
    
    # Create sample data
    create_sample_data()
    
    # Print information
    logger.info("=" * 60)
    logger.info("Retail ETL Pipeline with Apache Airflow")
    logger.info("=" * 60)
    logger.info("Features:")
    logger.info("- File sensor for automatic pipeline triggering")
    logger.info("- External API enrichment (currency, country, product)")
    logger.info("- Bronze → Silver → Gold data layers")
    logger.info("- Data quality monitoring and reporting")
    logger.info("- Web-based monitoring and management")
    logger.info("=" * 60)
    
    # Start components
    print(f"\n🚀 Starting Apache Airflow...")
    print(f"📊 Pipeline: Retail ETL with External API Enrichment")
    print(f"🌐 Airflow UI: http://127.0.0.1:8080")
    print(f"👤 Login: admin / admin123")
    print(f"📁 DAGs directory: {os.environ['AIRFLOW__CORE__DAGS_FOLDER']}")
    print(f"📁 Raw data directory: {settings.raw_data_path}")
    print(f"🔄 To trigger pipeline: Drop a CSV file in {settings.raw_data_path}")
    print(f"⏹️  To stop: Press Ctrl+C\n")
    
    # Start scheduler
    scheduler_process = start_airflow_scheduler()
    if not scheduler_process:
        logger.error("Failed to start Airflow scheduler")
        sys.exit(1)
    
    # Start web server
    webserver_process = start_airflow_webserver()
    if not webserver_process:
        logger.error("Failed to start Airflow web server")
        if scheduler_process:
            scheduler_process.terminate()
        sys.exit(1)
    
    try:
        # Wait for processes
        logger.info("Airflow is running. Press Ctrl+C to stop...")
        while True:
            # Check if processes are still running
            if scheduler_process.poll() is not None:
                logger.error("Scheduler process died")
                break
            if webserver_process.poll() is not None:
                logger.error("Web server process died") 
                break
            time.sleep(5)
            
    except KeyboardInterrupt:
        logger.info("Shutting down Airflow...")
        
        # Terminate processes
        if scheduler_process:
            scheduler_process.terminate()
            scheduler_process.wait()
        if webserver_process:
            webserver_process.terminate()
            webserver_process.wait()
            
        logger.info("Airflow stopped")


if __name__ == "__main__":
    main()