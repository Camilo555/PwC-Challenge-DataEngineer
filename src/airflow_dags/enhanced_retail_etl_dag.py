"""
Enhanced Retail ETL DAG with Spark Integration and Advanced Monitoring
This DAG orchestrates the complete retail data pipeline with comprehensive monitoring and error handling.
"""

from datetime import datetime, timedelta
from typing import Dict, Any, List
import os
import sys

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.http.sensors.http import HttpSensor
from airflow.sensors.filesystem import FileSensor
from airflow.models import Variable
from airflow.utils.dates import days_ago
from airflow.utils.email import send_email
from airflow.utils.trigger_rule import TriggerRule

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

from core.config import settings
from core.logging import get_logger
from monitoring import get_metrics_collector, get_alert_manager, trigger_custom_alert, AlertSeverity
from etl.bronze.spark_bronze import SparkBronzeProcessor
from etl.silver.spark_silver import SparkSilverProcessor
from etl.gold.spark_gold import SparkGoldProcessor
from etl.spark.session_manager import SparkSessionManager
from external_apis.enrichment_service import EnrichmentService

logger = get_logger(__name__)

# DAG Configuration
DEFAULT_ARGS = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'max_active_runs': 1,
    'catchup': False,
    'email': ['data-team@company.com'],
}

# DAG Definition
dag = DAG(
    'enhanced_retail_etl_pipeline',
    default_args=DEFAULT_ARGS,
    description='Enhanced retail data ETL pipeline with Spark and comprehensive monitoring',
    schedule_interval='@daily',
    tags=['etl', 'retail', 'spark', 'production'],
    doc_md=__doc__,
)

# ================================
# UTILITY FUNCTIONS
# ================================

def send_pipeline_notification(context: Dict[str, Any], status: str = "success"):
    """Send pipeline status notification"""
    task_instance = context['task_instance']
    dag_run = context['dag_run']
    
    metrics_collector = get_metrics_collector()
    
    if status == "failure":
        trigger_custom_alert(
            title=f"ETL Pipeline Failed: {dag_run.dag_id}",
            description=f"Task {task_instance.task_id} failed at {datetime.now()}",
            severity=AlertSeverity.HIGH,
            source="airflow",
            tags={"dag_id": dag_run.dag_id, "task_id": task_instance.task_id}
        )
    
    # Record metrics
    metrics_collector.record_etl_metrics(
        pipeline_name=dag_run.dag_id,
        stage="notification",
        records_processed=0,
        records_failed=0 if status == "success" else 1,
        processing_time=0,
        data_quality_score=100 if status == "success" else 0
    )

def check_data_availability(**context) -> bool:
    """Check if source data is available for processing"""
    data_path = Variable.get("raw_data_path", default_var="./data/raw")
    required_files = ["online_retail_II.xlsx", "retail_sample.csv"]
    
    missing_files = []
    for file_name in required_files:
        file_path = os.path.join(data_path, file_name)
        if not os.path.exists(file_path):
            missing_files.append(file_name)
    
    if missing_files:
        logger.warning(f"Missing data files: {missing_files}")
        trigger_custom_alert(
            title="Missing Source Data Files",
            description=f"Files not found: {', '.join(missing_files)}",
            severity=AlertSeverity.MEDIUM,
            source="airflow"
        )
        return False
    
    logger.info("All required data files are available")
    return True

def validate_database_connection(**context) -> bool:
    """Validate database connectivity"""
    try:
        postgres_hook = PostgresHook(postgres_conn_id='postgres_default')
        connection = postgres_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute("SELECT 1")
        result = cursor.fetchone()
        cursor.close()
        connection.close()
        
        logger.info("Database connection validated successfully")
        return True
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        trigger_custom_alert(
            title="Database Connection Failed",
            description=f"Unable to connect to database: {str(e)}",
            severity=AlertSeverity.CRITICAL,
            source="airflow"
        )
        return False

def monitor_system_resources(**context) -> Dict[str, Any]:
    """Monitor system resources and alert if thresholds exceeded"""
    metrics_collector = get_metrics_collector()
    system_metrics = metrics_collector.collect_system_metrics()
    
    alerts = []
    
    # Check CPU usage
    if system_metrics.cpu_usage_percent > 90:
        alerts.append(f"High CPU usage: {system_metrics.cpu_usage_percent:.1f}%")
    
    # Check memory usage
    if system_metrics.memory_usage_percent > 85:
        alerts.append(f"High memory usage: {system_metrics.memory_usage_percent:.1f}%")
    
    # Check disk usage
    if system_metrics.disk_usage_percent > 80:
        alerts.append(f"High disk usage: {system_metrics.disk_usage_percent:.1f}%")
    
    if alerts:
        trigger_custom_alert(
            title="System Resource Alert",
            description="; ".join(alerts),
            severity=AlertSeverity.MEDIUM,
            source="airflow"
        )
    
    return {
        'cpu_percent': system_metrics.cpu_usage_percent,
        'memory_percent': system_metrics.memory_usage_percent,
        'disk_percent': system_metrics.disk_usage_percent
    }

# ================================
# ETL PROCESSING FUNCTIONS
# ================================

def process_bronze_layer(**context) -> Dict[str, Any]:
    """Process bronze layer with Spark"""
    logger.info("Starting Bronze layer processing with Spark")
    
    start_time = datetime.now()
    metrics_collector = get_metrics_collector()
    
    try:
        # Initialize Spark session
        spark_manager = SparkSessionManager()
        spark = spark_manager.get_or_create_session("bronze_processing")
        
        # Initialize bronze processor
        bronze_processor = SparkBronzeProcessor(spark_session=spark)
        
        # Process data
        result = bronze_processor.process_all_sources()
        
        processing_time = (datetime.now() - start_time).total_seconds()
        
        # Record metrics
        metrics_collector.record_etl_metrics(
            pipeline_name="enhanced_retail_etl_pipeline",
            stage="bronze",
            records_processed=result.get('total_records', 0),
            records_failed=result.get('failed_records', 0),
            processing_time=processing_time,
            data_quality_score=result.get('data_quality_score', 0),
            error_count=result.get('error_count', 0)
        )
        
        logger.info(f"Bronze layer processing completed: {result}")
        return result
        
    except Exception as e:
        logger.error(f"Bronze layer processing failed: {e}")
        trigger_custom_alert(
            title="Bronze Layer Processing Failed",
            description=f"Error in bronze layer: {str(e)}",
            severity=AlertSeverity.HIGH,
            source="airflow"
        )
        raise
    finally:
        spark_manager.stop_session()

def process_silver_layer(**context) -> Dict[str, Any]:
    """Process silver layer with Spark and data quality validation"""
    logger.info("Starting Silver layer processing with Spark")
    
    start_time = datetime.now()
    metrics_collector = get_metrics_collector()
    
    try:
        # Initialize Spark session
        spark_manager = SparkSessionManager()
        spark = spark_manager.get_or_create_session("silver_processing")
        
        # Initialize silver processor
        silver_processor = SparkSilverProcessor(spark_session=spark)
        
        # Process data with validation
        result = silver_processor.process_with_quality_checks()
        
        processing_time = (datetime.now() - start_time).total_seconds()
        
        # Record metrics
        metrics_collector.record_etl_metrics(
            pipeline_name="enhanced_retail_etl_pipeline",
            stage="silver",
            records_processed=result.get('total_records', 0),
            records_failed=result.get('failed_records', 0),
            processing_time=processing_time,
            data_quality_score=result.get('data_quality_score', 0),
            error_count=result.get('error_count', 0)
        )
        
        # Check data quality threshold
        quality_threshold = float(Variable.get("data_quality_threshold", default_var="0.95"))
        if result.get('data_quality_score', 0) < quality_threshold:
            trigger_custom_alert(
                title="Data Quality Below Threshold",
                description=f"Silver layer quality score: {result.get('data_quality_score', 0):.2f}, threshold: {quality_threshold}",
                severity=AlertSeverity.MEDIUM,
                source="airflow"
            )
        
        logger.info(f"Silver layer processing completed: {result}")
        return result
        
    except Exception as e:
        logger.error(f"Silver layer processing failed: {e}")
        trigger_custom_alert(
            title="Silver Layer Processing Failed",
            description=f"Error in silver layer: {str(e)}",
            severity=AlertSeverity.HIGH,
            source="airflow"
        )
        raise
    finally:
        spark_manager.stop_session()

def enrich_with_external_apis(**context) -> Dict[str, Any]:
    """Enrich data with external APIs"""
    logger.info("Starting external API enrichment")
    
    if not Variable.get("enable_external_enrichment", default_var="false").lower() == "true":
        logger.info("External enrichment disabled, skipping...")
        return {"status": "skipped", "reason": "disabled"}
    
    start_time = datetime.now()
    
    try:
        enrichment_service = EnrichmentService()
        result = enrichment_service.enrich_all_data()
        
        processing_time = (datetime.now() - start_time).total_seconds()
        
        logger.info(f"External API enrichment completed: {result}")
        return result
        
    except Exception as e:
        logger.error(f"External API enrichment failed: {e}")
        trigger_custom_alert(
            title="External API Enrichment Failed",
            description=f"Error in external enrichment: {str(e)}",
            severity=AlertSeverity.MEDIUM,
            source="airflow"
        )
        # Don't fail the pipeline for enrichment issues
        return {"status": "failed", "error": str(e)}

def process_gold_layer(**context) -> Dict[str, Any]:
    """Process gold layer with advanced analytics"""
    logger.info("Starting Gold layer processing with Spark")
    
    start_time = datetime.now()
    metrics_collector = get_metrics_collector()
    
    try:
        # Initialize Spark session
        spark_manager = SparkSessionManager()
        spark = spark_manager.get_or_create_session("gold_processing")
        
        # Initialize gold processor
        gold_processor = SparkGoldProcessor(spark_session=spark)
        
        # Process data with analytics
        result = gold_processor.build_analytics_tables()
        
        processing_time = (datetime.now() - start_time).total_seconds()
        
        # Record metrics
        metrics_collector.record_etl_metrics(
            pipeline_name="enhanced_retail_etl_pipeline",
            stage="gold",
            records_processed=result.get('total_records', 0),
            records_failed=result.get('failed_records', 0),
            processing_time=processing_time,
            data_quality_score=result.get('data_quality_score', 0),
            error_count=result.get('error_count', 0)
        )
        
        logger.info(f"Gold layer processing completed: {result}")
        return result
        
    except Exception as e:
        logger.error(f"Gold layer processing failed: {e}")
        trigger_custom_alert(
            title="Gold Layer Processing Failed",
            description=f"Error in gold layer: {str(e)}",
            severity=AlertSeverity.HIGH,
            source="airflow"
        )
        raise
    finally:
        spark_manager.stop_session()

def update_vector_search_index(**context) -> Dict[str, Any]:
    """Update vector search index with new data"""
    logger.info("Updating vector search index")
    
    if not Variable.get("enable_vector_search", default_var="false").lower() == "true":
        logger.info("Vector search disabled, skipping...")
        return {"status": "skipped", "reason": "disabled"}
    
    try:
        # Vector search indexing logic would go here
        logger.info("Vector search index updated successfully")
        return {"status": "success", "indexed_documents": 0}
        
    except Exception as e:
        logger.error(f"Vector search indexing failed: {e}")
        trigger_custom_alert(
            title="Vector Search Indexing Failed",
            description=f"Error updating vector search index: {str(e)}",
            severity=AlertSeverity.MEDIUM,
            source="airflow"
        )
        return {"status": "failed", "error": str(e)}

def generate_data_quality_report(**context) -> Dict[str, Any]:
    """Generate comprehensive data quality report"""
    logger.info("Generating data quality report")
    
    try:
        # Get metrics from previous tasks
        bronze_result = context['task_instance'].xcom_pull(task_ids='process_bronze_layer')
        silver_result = context['task_instance'].xcom_pull(task_ids='process_silver_layer')
        gold_result = context['task_instance'].xcom_pull(task_ids='process_gold_layer')
        
        report = {
            'timestamp': datetime.now().isoformat(),
            'pipeline_id': context['dag_run'].run_id,
            'bronze_quality': bronze_result.get('data_quality_score', 0) if bronze_result else 0,
            'silver_quality': silver_result.get('data_quality_score', 0) if silver_result else 0,
            'gold_quality': gold_result.get('data_quality_score', 0) if gold_result else 0,
            'total_records_processed': sum([
                bronze_result.get('total_records', 0) if bronze_result else 0,
                silver_result.get('total_records', 0) if silver_result else 0,
                gold_result.get('total_records', 0) if gold_result else 0
            ]),
            'total_errors': sum([
                bronze_result.get('error_count', 0) if bronze_result else 0,
                silver_result.get('error_count', 0) if silver_result else 0,
                gold_result.get('error_count', 0) if gold_result else 0
            ])
        }
        
        # Save report
        report_path = f"./reports/data_quality/quality_report_{context['ds']}.json"
        os.makedirs(os.path.dirname(report_path), exist_ok=True)
        
        import json
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2)
        
        logger.info(f"Data quality report generated: {report_path}")
        return report
        
    except Exception as e:
        logger.error(f"Data quality report generation failed: {e}")
        return {"status": "failed", "error": str(e)}

# ================================
# DAG TASK DEFINITIONS
# ================================

# Start task
start_pipeline = DummyOperator(
    task_id='start_pipeline',
    dag=dag,
)

# Pre-flight checks
check_data_files = PythonOperator(
    task_id='check_data_availability',
    python_callable=check_data_availability,
    dag=dag,
)

validate_db_connection = PythonOperator(
    task_id='validate_database_connection',
    python_callable=validate_database_connection,
    dag=dag,
)

monitor_resources = PythonOperator(
    task_id='monitor_system_resources',
    python_callable=monitor_system_resources,
    dag=dag,
)

# ETL Processing tasks
bronze_processing = PythonOperator(
    task_id='process_bronze_layer',
    python_callable=process_bronze_layer,
    dag=dag,
)

silver_processing = PythonOperator(
    task_id='process_silver_layer',
    python_callable=process_silver_layer,
    dag=dag,
)

external_enrichment = PythonOperator(
    task_id='enrich_with_external_apis',
    python_callable=enrich_with_external_apis,
    dag=dag,
    trigger_rule=TriggerRule.NONE_FAILED,  # Continue even if previous tasks have warnings
)

gold_processing = PythonOperator(
    task_id='process_gold_layer',
    python_callable=process_gold_layer,
    dag=dag,
)

# Post-processing tasks
update_vector_index = PythonOperator(
    task_id='update_vector_search_index',
    python_callable=update_vector_search_index,
    dag=dag,
    trigger_rule=TriggerRule.NONE_FAILED,
)

generate_quality_report = PythonOperator(
    task_id='generate_data_quality_report',
    python_callable=generate_data_quality_report,
    dag=dag,
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
)

# Health check
api_health_check = HttpSensor(
    task_id='check_api_health',
    http_conn_id='api_default',
    endpoint='/api/v1/health',
    timeout=30,
    poke_interval=10,
    dag=dag,
)

# Success notification
success_notification = PythonOperator(
    task_id='send_success_notification',
    python_callable=send_pipeline_notification,
    op_kwargs={'status': 'success'},
    dag=dag,
    trigger_rule=TriggerRule.ALL_SUCCESS,
)

# Failure notification
failure_notification = PythonOperator(
    task_id='send_failure_notification',
    python_callable=send_pipeline_notification,
    op_kwargs={'status': 'failure'},
    dag=dag,
    trigger_rule=TriggerRule.ONE_FAILED,
)

# End task
end_pipeline = DummyOperator(
    task_id='end_pipeline',
    dag=dag,
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
)

# ================================
# DAG TASK DEPENDENCIES
# ================================

# Pre-flight checks
start_pipeline >> [check_data_files, validate_db_connection, monitor_resources]

# Main ETL flow
[check_data_files, validate_db_connection] >> bronze_processing
bronze_processing >> silver_processing
silver_processing >> [external_enrichment, gold_processing]
[external_enrichment, gold_processing] >> update_vector_index

# Quality and monitoring
[bronze_processing, silver_processing, gold_processing] >> generate_quality_report
update_vector_index >> api_health_check

# Notifications and completion
[api_health_check, generate_quality_report] >> success_notification
success_notification >> end_pipeline

# Failure handling
[bronze_processing, silver_processing, gold_processing, api_health_check] >> failure_notification
failure_notification >> end_pipeline

# Documentation
dag.doc_md = """
# Enhanced Retail ETL Pipeline

This DAG orchestrates a comprehensive retail data processing pipeline with the following features:

## Pipeline Stages
1. **Pre-flight Checks**: Data availability, database connectivity, system resources
2. **Bronze Layer**: Raw data ingestion with Spark processing
3. **Silver Layer**: Data cleaning and quality validation
4. **External Enrichment**: API-based data enrichment (optional)
5. **Gold Layer**: Analytics-ready data transformation
6. **Vector Search**: Search index updates (optional)
7. **Quality Reporting**: Comprehensive data quality assessment

## Monitoring and Alerting
- Real-time system resource monitoring
- Data quality threshold validation
- Comprehensive error handling and alerting
- Integration with monitoring systems

## Configuration Variables
- `raw_data_path`: Path to source data files
- `data_quality_threshold`: Minimum acceptable quality score (default: 0.95)
- `enable_external_enrichment`: Enable/disable external API enrichment
- `enable_vector_search`: Enable/disable vector search indexing

## Notifications
- Success/failure notifications via email and alerts
- Comprehensive metrics collection
- Integration with monitoring dashboards
"""