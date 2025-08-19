"""
Advanced Airflow Configuration
Provides comprehensive Airflow settings for enterprise deployment
"""
from __future__ import annotations

from datetime import timedelta, datetime
from typing import Dict, List, Optional, Any
from pathlib import Path

from pydantic import Field, validator
from pydantic_settings import BaseSettings

from .base_config import BaseConfig, Environment


class AirflowConfig(BaseSettings):
    """Enhanced Airflow configuration with production-ready settings."""
    
    # Core Airflow settings
    airflow_home: Optional[str] = Field(default=None, env="AIRFLOW_HOME")
    dags_folder: str = Field(default="src/airflow_dags", env="AIRFLOW__CORE__DAGS_FOLDER")
    base_log_folder: str = Field(default="logs", env="AIRFLOW__LOGGING__BASE_LOG_FOLDER")
    
    # Database configuration
    sql_alchemy_conn: str = Field(
        default="sqlite:///airflow.db",
        env="AIRFLOW__DATABASE__SQL_ALCHEMY_CONN"
    )
    
    # Executor configuration
    executor: str = Field(default="LocalExecutor", env="AIRFLOW__CORE__EXECUTOR")
    max_active_runs_per_dag: int = Field(default=3, env="AIRFLOW__CORE__MAX_ACTIVE_RUNS_PER_DAG")
    max_active_tasks_per_dag: int = Field(default=16, env="AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG")
    parallelism: int = Field(default=32, env="AIRFLOW__CORE__PARALLELISM")
    
    # Security settings
    authenticate: bool = Field(default=True, env="AIRFLOW__WEBSERVER__AUTHENTICATE")
    auth_backend: str = Field(
        default="airflow.contrib.auth.backends.password_auth",
        env="AIRFLOW__WEBSERVER__AUTH_BACKEND"
    )
    secret_key: str = Field(default="", env="AIRFLOW__WEBSERVER__SECRET_KEY")
    
    # Web server configuration
    web_server_host: str = Field(default="0.0.0.0", env="AIRFLOW__WEBSERVER__WEB_SERVER_HOST")
    web_server_port: int = Field(default=8080, env="AIRFLOW__WEBSERVER__WEB_SERVER_PORT")
    workers: int = Field(default=4, env="AIRFLOW__WEBSERVER__WORKERS")
    
    # Scheduler configuration
    scheduler_heartbeat_sec: int = Field(default=5, env="AIRFLOW__SCHEDULER__SCHEDULER_HEARTBEAT_SEC")
    min_file_process_interval: int = Field(default=30, env="AIRFLOW__SCHEDULER__MIN_FILE_PROCESS_INTERVAL")
    dag_dir_list_interval: int = Field(default=300, env="AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL")
    max_threads: int = Field(default=2, env="AIRFLOW__SCHEDULER__MAX_THREADS")
    
    # Email configuration
    smtp_host: Optional[str] = Field(default=None, env="AIRFLOW__SMTP__SMTP_HOST")
    smtp_port: int = Field(default=587, env="AIRFLOW__SMTP__SMTP_PORT")
    smtp_user: Optional[str] = Field(default=None, env="AIRFLOW__SMTP__SMTP_USER")
    smtp_password: Optional[str] = Field(default=None, env="AIRFLOW__SMTP__SMTP_PASSWORD")
    smtp_mail_from: str = Field(default="airflow@example.com", env="AIRFLOW__SMTP__SMTP_MAIL_FROM")
    
    # Slack configuration
    slack_webhook_url: Optional[str] = Field(default=None, env="SLACK_WEBHOOK_URL")
    slack_channel: str = Field(default="#data-alerts", env="SLACK_CHANNEL")
    slack_username: str = Field(default="Airflow", env="SLACK_USERNAME")
    
    # Monitoring and logging
    enable_metrics: bool = Field(default=True, env="AIRFLOW__METRICS__STATSD_ON")
    statsd_host: str = Field(default="localhost", env="AIRFLOW__METRICS__STATSD_HOST")
    statsd_port: int = Field(default=8125, env="AIRFLOW__METRICS__STATSD_PORT")
    
    # Retry configuration
    default_retries: int = Field(default=3, env="AIRFLOW_DEFAULT_RETRIES")
    default_retry_delay_minutes: int = Field(default=5, env="AIRFLOW_DEFAULT_RETRY_DELAY_MINUTES")
    max_retry_delay_minutes: int = Field(default=60, env="AIRFLOW_MAX_RETRY_DELAY_MINUTES")
    
    # Task timeout configuration
    default_task_timeout_hours: int = Field(default=2, env="AIRFLOW_DEFAULT_TASK_TIMEOUT_HOURS")
    etl_task_timeout_hours: int = Field(default=4, env="AIRFLOW_ETL_TASK_TIMEOUT_HOURS")
    
    # External services
    enable_external_apis: bool = Field(default=True, env="AIRFLOW_ENABLE_EXTERNAL_APIS")
    api_timeout_seconds: int = Field(default=30, env="AIRFLOW_API_TIMEOUT_SECONDS")
    
    # Data quality thresholds
    min_quality_score: float = Field(default=0.8, env="AIRFLOW_MIN_QUALITY_SCORE")
    max_null_percentage: float = Field(default=0.1, env="AIRFLOW_MAX_NULL_PERCENTAGE")
    
    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False
        extra = "allow"
    
    @validator("airflow_home", pre=True)
    def set_airflow_home(cls, v: Optional[str]) -> str:
        """Set AIRFLOW_HOME if not provided."""
        if v is None:
            v = str(Path.cwd() / "airflow_home")
        Path(v).mkdir(parents=True, exist_ok=True)
        return v
    
    def get_dag_default_args(self, base_config: BaseConfig) -> Dict[str, Any]:
        """Get default arguments for DAG creation."""
        return {
            'owner': 'data-team',
            'depends_on_past': False,
            'start_date': datetime(2024, 1, 1),
            'email_on_failure': self.smtp_host is not None,
            'email_on_retry': False,
            'retries': self.default_retries,
            'retry_delay': timedelta(minutes=self.default_retry_delay_minutes),
            'retry_exponential_backoff': True,
            'max_retry_delay': timedelta(minutes=self.max_retry_delay_minutes),
            'execution_timeout': timedelta(hours=self.default_task_timeout_hours),
            'on_failure_callback': self._get_failure_callback(),
            'on_success_callback': self._get_success_callback(),
            'on_retry_callback': self._get_retry_callback(),
            'pool': self._get_default_pool(base_config.environment),
            'queue': self._get_default_queue(base_config.environment),
        }
    
    def _get_failure_callback(self):
        """Get failure callback function."""
        def task_failure_callback(context):
            """Handle task failure notifications."""
            try:
                # Slack notification
                if self.slack_webhook_url:
                    self._send_slack_notification(context, "FAILED")
                
                # Email notification 
                if self.smtp_host and context.get('task_instance'):
                    self._send_email_notification(context, "FAILED")
                    
            except Exception as e:
                print(f"Error in failure callback: {e}")
        
        return task_failure_callback
    
    def _get_success_callback(self):
        """Get success callback function."""
        def task_success_callback(context):
            """Handle task success notifications."""
            # Only notify on final task success or critical milestones
            task_id = context.get('task_instance', {}).task_id
            if task_id in ['gold_layer_etl', 'data_quality_check']:
                try:
                    if self.slack_webhook_url:
                        self._send_slack_notification(context, "SUCCESS")
                except Exception as e:
                    print(f"Error in success callback: {e}")
        
        return task_success_callback
    
    def _get_retry_callback(self):
        """Get retry callback function."""
        def task_retry_callback(context):
            """Handle task retry notifications."""
            try:
                task_instance = context.get('task_instance')
                if task_instance and task_instance.try_number >= 2:
                    # Only notify after second retry
                    if self.slack_webhook_url:
                        self._send_slack_notification(context, "RETRY")
            except Exception as e:
                print(f"Error in retry callback: {e}")
        
        return task_retry_callback
    
    def _send_slack_notification(self, context: Dict[str, Any], status: str) -> None:
        """Send Slack notification."""
        try:
            import requests
            
            task_instance = context.get('task_instance')
            dag_run = context.get('dag_run')
            
            color_map = {
                "SUCCESS": "good",
                "FAILED": "danger", 
                "RETRY": "warning"
            }
            
            message = {
                "channel": self.slack_channel,
                "username": self.slack_username,
                "attachments": [{
                    "color": color_map.get(status, "good"),
                    "title": f"Airflow Task {status}",
                    "fields": [
                        {"title": "DAG", "value": dag_run.dag_id if dag_run else "Unknown", "short": True},
                        {"title": "Task", "value": task_instance.task_id if task_instance else "Unknown", "short": True},
                        {"title": "Execution Date", "value": str(dag_run.execution_date) if dag_run else "Unknown", "short": True},
                        {"title": "Log URL", "value": task_instance.log_url if task_instance else "N/A", "short": False}
                    ]
                }]
            }
            
            requests.post(self.slack_webhook_url, json=message, timeout=10)
            
        except Exception as e:
            print(f"Failed to send Slack notification: {e}")
    
    def _send_email_notification(self, context: Dict[str, Any], status: str) -> None:
        """Send email notification."""
        # Implementation would depend on email library
        pass
    
    def _get_default_pool(self, environment: Environment) -> str:
        """Get default pool based on environment."""
        pool_map = {
            Environment.DEVELOPMENT: "default_pool",
            Environment.TESTING: "test_pool",
            Environment.STAGING: "staging_pool",
            Environment.PRODUCTION: "production_pool"
        }
        return pool_map.get(environment, "default_pool")
    
    def _get_default_queue(self, environment: Environment) -> str:
        """Get default queue based on environment."""
        queue_map = {
            Environment.DEVELOPMENT: "default",
            Environment.TESTING: "test",
            Environment.STAGING: "staging", 
            Environment.PRODUCTION: "production"
        }
        return queue_map.get(environment, "default")
    
    def get_connection_configs(self) -> Dict[str, Dict[str, Any]]:
        """Get Airflow connection configurations."""
        return {
            "postgres_default": {
                "conn_type": "postgres",
                "host": "localhost",
                "schema": "retail_dw",
                "login": "airflow",
                "password": "airflow",
                "port": 5432
            },
            "spark_default": {
                "conn_type": "spark", 
                "host": "spark://localhost:7077",
                "extra": {
                    "master": "spark://localhost:7077",
                    "deploy-mode": "client"
                }
            },
            "slack_default": {
                "conn_type": "http",
                "host": "hooks.slack.com",
                "password": self.slack_webhook_url
            }
        }
    
    def get_variable_configs(self) -> Dict[str, Any]:
        """Get Airflow variable configurations."""
        return {
            "data_quality_min_score": self.min_quality_score,
            "etl_max_null_percentage": self.max_null_percentage,
            "enable_external_apis": self.enable_external_apis,
            "api_timeout_seconds": self.api_timeout_seconds,
            "environment": "production",  # Will be overridden by base_config
            "bronze_path": "data/bronze",
            "silver_path": "data/silver", 
            "gold_path": "data/gold"
        }
    
    def get_pool_configs(self) -> Dict[str, Dict[str, Any]]:
        """Get Airflow pool configurations."""
        return {
            "etl_pool": {
                "slots": 4,
                "description": "Pool for ETL tasks"
            },
            "api_pool": {
                "slots": 2,
                "description": "Pool for external API tasks"
            },
            "quality_pool": {
                "slots": 2,
                "description": "Pool for data quality tasks"
            }
        }
    
    def generate_airflow_cfg(self, base_config: BaseConfig) -> str:
        """Generate airflow.cfg content."""
        config_template = f"""
[core]
dags_folder = {self.dags_folder}
base_log_folder = {self.base_log_folder}
executor = {self.executor}
sql_alchemy_conn = {self.sql_alchemy_conn}
max_active_runs_per_dag = {self.max_active_runs_per_dag}
max_active_tasks_per_dag = {self.max_active_tasks_per_dag}
parallelism = {self.parallelism}
load_examples = False
plugins_folder = plugins

[database]
sql_alchemy_conn = {self.sql_alchemy_conn}
sql_alchemy_pool_enabled = True
sql_alchemy_pool_size = 5
sql_alchemy_max_overflow = 10

[webserver]
authenticate = {str(self.authenticate).lower()}
auth_backend = {self.auth_backend}
secret_key = {self.secret_key}
web_server_host = {self.web_server_host}
web_server_port = {self.web_server_port}
workers = {self.workers}
expose_config = True

[scheduler]
scheduler_heartbeat_sec = {self.scheduler_heartbeat_sec}
min_file_process_interval = {self.min_file_process_interval}
dag_dir_list_interval = {self.dag_dir_list_interval}
max_threads = {self.max_threads}

[smtp]
smtp_host = {self.smtp_host or ''}
smtp_starttls = True
smtp_ssl = False
smtp_port = {self.smtp_port}
smtp_user = {self.smtp_user or ''}
smtp_password = {self.smtp_password or ''}
smtp_mail_from = {self.smtp_mail_from}

[metrics]
statsd_on = {str(self.enable_metrics).lower()}
statsd_host = {self.statsd_host}
statsd_port = {self.statsd_port}
statsd_prefix = airflow

[logging]
remote_logging = False
logging_level = {'DEBUG' if base_config.debug else 'INFO'}
fab_logging_level = WARN
log_processor_filename_template = {{{{ ti.dag_id }}}}/{{{{ ti.task_id }}}}/{{{{ ts }}}}/{{{{ try_number }}}}.log

[celery]
broker_url = redis://localhost:6379/0
result_backend = redis://localhost:6379/0
worker_concurrency = 16

[kubernetes]
namespace = airflow
worker_container_repository = apache/airflow
worker_container_tag = latest
"""
        return config_template.strip()