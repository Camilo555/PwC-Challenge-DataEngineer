"""
Airflow configuration and utilities for the retail ETL pipeline.

This module provides configuration management, custom operators, and utility functions
specifically designed for the PwC Data Engineering Challenge Airflow implementation.
"""

import os
import sys
from pathlib import Path
from typing import Any

# Add src directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))


from core.config import settings
from core.logging import get_logger

logger = get_logger(__name__)


class AirflowConfig:
    """Configuration manager for Airflow DAGs."""

    def __init__(self):
        """Initialize Airflow configuration."""
        self.dag_config = self._load_dag_config()

    def _load_dag_config(self) -> dict[str, Any]:
        """Load DAG configuration from Airflow Variables or environment."""
        return {
            'email_alerts': os.getenv('AIRFLOW_EMAIL_ALERTS', 'false').lower() == 'true',
            'email_recipients': os.getenv('AIRFLOW_EMAIL_RECIPIENTS', 'admin@company.com').split(','),
            'max_active_runs': int(os.getenv('AIRFLOW_MAX_ACTIVE_RUNS', '1')),
            'default_retries': int(os.getenv('AIRFLOW_DEFAULT_RETRIES', '3')),
            'retry_delay_minutes': int(os.getenv('AIRFLOW_RETRY_DELAY_MINUTES', '5')),
            'execution_timeout_hours': int(os.getenv('AIRFLOW_EXECUTION_TIMEOUT_HOURS', '2')),
            'enable_slack_notifications': os.getenv('AIRFLOW_SLACK_NOTIFICATIONS', 'false').lower() == 'true',
            'slack_webhook_url': os.getenv('AIRFLOW_SLACK_WEBHOOK_URL', ''),
        }

    def get_email_config(self) -> dict[str, Any]:
        """Get email notification configuration."""
        return {
            'email_on_failure': self.dag_config['email_alerts'],
            'email_on_retry': False,
            'email': self.dag_config['email_recipients'] if self.dag_config['email_alerts'] else [],
        }

    def get_retry_config(self) -> dict[str, Any]:
        """Get retry configuration."""
        from datetime import timedelta

        return {
            'retries': self.dag_config['default_retries'],
            'retry_delay': timedelta(minutes=self.dag_config['retry_delay_minutes']),
            'retry_exponential_backoff': True,
            'max_retry_delay': timedelta(hours=1),
        }

    def get_execution_config(self) -> dict[str, Any]:
        """Get execution configuration."""
        from datetime import timedelta

        return {
            'max_active_runs': self.dag_config['max_active_runs'],
            'execution_timeout': timedelta(hours=self.dag_config['execution_timeout_hours']),
            'catchup': False,
        }


def setup_airflow_logging():
    """Configure enhanced logging for Airflow tasks."""
    import logging

    # Create custom formatter for Airflow logs
    formatter = logging.Formatter(
        '[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s'
    )

    # Configure root logger
    root_logger = logging.getLogger()

    # Add custom handler if not already present
    if not any(isinstance(h, logging.StreamHandler) for h in root_logger.handlers):
        handler = logging.StreamHandler()
        handler.setFormatter(formatter)
        root_logger.addHandler(handler)

    logger.info("Enhanced Airflow logging configured")


def send_slack_notification(context: dict[str, Any], message: str, webhook_url: str = None):
    """
    Send Slack notification for DAG events.
    
    Args:
        context: Airflow context
        message: Message to send
        webhook_url: Slack webhook URL (optional, uses config if not provided)
    """
    if not webhook_url:
        webhook_url = AirflowConfig().dag_config.get('slack_webhook_url')

    if not webhook_url:
        logger.warning("Slack webhook URL not configured, skipping notification")
        return

    try:
        import requests

        dag_id = context['dag'].dag_id
        task_id = context['task'].task_id
        execution_date = context['execution_date']
        run_id = context['dag_run'].run_id

        payload = {
            "text": f"*{message}*",
            "attachments": [
                {
                    "color": "danger" if "failed" in message.lower() else "good",
                    "fields": [
                        {"title": "DAG", "value": dag_id, "short": True},
                        {"title": "Task", "value": task_id, "short": True},
                        {"title": "Execution Date", "value": str(execution_date), "short": True},
                        {"title": "Run ID", "value": run_id, "short": True},
                    ]
                }
            ]
        }

        response = requests.post(webhook_url, json=payload, timeout=10)
        response.raise_for_status()

        logger.info(f"Slack notification sent successfully: {message}")

    except Exception as e:
        logger.error(f"Failed to send Slack notification: {e}")


def task_failure_callback(context: dict[str, Any]):
    """Callback function for task failures."""
    task_id = context['task_instance'].task_id
    dag_id = context['dag'].dag_id
    execution_date = context['execution_date']

    message = f"Task Failed: {dag_id}.{task_id}"
    logger.error(f"Task failure callback triggered: {message}")

    # Send Slack notification if enabled
    config = AirflowConfig()
    if config.dag_config['enable_slack_notifications']:
        send_slack_notification(context, message)

    # Additional failure handling logic can be added here
    # e.g., creating tickets, sending detailed reports, etc.


def task_success_callback(context: dict[str, Any]):
    """Callback function for task success (optional)."""
    task_id = context['task_instance'].task_id
    dag_id = context['dag'].dag_id

    logger.info(f"Task completed successfully: {dag_id}.{task_id}")

    # Optional success notifications for critical tasks
    if task_id in ['silver_to_gold', 'upload_to_supabase', 'generate_report']:
        config = AirflowConfig()
        if config.dag_config['enable_slack_notifications']:
            message = f"Critical Task Completed: {dag_id}.{task_id}"
            send_slack_notification(context, message)


def validate_dag_dependencies():
    """Validate that all required dependencies are available."""
    validation_results = {
        'python_packages': [],
        'directories': [],
        'configuration': [],
        'all_valid': True
    }

    # Check Python packages
    required_packages = [
        'pandas', 'sqlalchemy', 'fastapi', 'pyspark'
    ]

    for package in required_packages:
        try:
            __import__(package)
            validation_results['python_packages'].append(f"✓ {package}")
        except ImportError:
            validation_results['python_packages'].append(f"✗ {package}")
            validation_results['all_valid'] = False

    # Check required directories
    required_dirs = [
        settings.raw_data_path,
        settings.bronze_path,
        settings.silver_path,
        settings.gold_path,
    ]

    for dir_path in required_dirs:
        if Path(dir_path).exists():
            validation_results['directories'].append(f"✓ {dir_path}")
        else:
            validation_results['directories'].append(f"✗ {dir_path}")
            validation_results['all_valid'] = False

    # Check configuration
    config_checks = [
        ('DATABASE_URL', bool(settings.database_url)),
        ('API_PORT', bool(settings.api_port)),
        ('ENVIRONMENT', bool(settings.environment)),
    ]

    for config_name, config_valid in config_checks:
        if config_valid:
            validation_results['configuration'].append(f"✓ {config_name}")
        else:
            validation_results['configuration'].append(f"✗ {config_name}")
            validation_results['all_valid'] = False

    logger.info(f"DAG dependency validation: {'PASSED' if validation_results['all_valid'] else 'FAILED'}")
    return validation_results


def create_data_quality_alert(context: dict[str, Any], quality_score: float, threshold: float):
    """Create data quality alert if score is below threshold."""
    if quality_score < threshold:
        message = f"Data Quality Alert: Score {quality_score:.1f}% below threshold {threshold:.1f}%"

        # Log critical alert
        logger.error(message)

        # Send notifications
        config = AirflowConfig()
        if config.dag_config['enable_slack_notifications']:
            send_slack_notification(context, message)

        # Could also trigger PagerDuty, email alerts, etc.
        return True

    return False


def get_dag_default_args() -> dict[str, Any]:
    """Get standardized default arguments for DAGs."""
    from airflow.utils.dates import days_ago

    config = AirflowConfig()

    default_args = {
        'owner': 'pwc-data-engineering',
        'depends_on_past': False,
        'start_date': days_ago(1),
        'catchup': False,
        'on_failure_callback': task_failure_callback,
    }

    # Add email configuration
    default_args.update(config.get_email_config())

    # Add retry configuration
    default_args.update(config.get_retry_config())

    # Add execution configuration
    default_args.update(config.get_execution_config())

    return default_args


# Initialize configuration when module is imported
airflow_config = AirflowConfig()

# Setup enhanced logging
setup_airflow_logging()

logger.info("Airflow configuration module initialized")
