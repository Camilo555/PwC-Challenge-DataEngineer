"""
Memory Monitoring Configuration
==============================

Configuration management for enterprise memory monitoring and optimization.
Provides environment-specific settings, performance tuning, and integration options.

Author: Enterprise Data Engineering Team
Created: 2025-09-18
"""

import os
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional

from pydantic import BaseSettings, Field, validator


class MonitoringEnvironment(Enum):
    """Deployment environments with different monitoring configurations."""
    DEVELOPMENT = "development"
    TESTING = "testing"
    STAGING = "staging"
    PRODUCTION = "production"


class OptimizationMode(Enum):
    """Memory optimization modes."""
    DISABLED = "disabled"
    MANUAL = "manual"
    AUTOMATIC = "automatic"
    AGGRESSIVE = "aggressive"


@dataclass
class MemoryThresholdConfig:
    """Memory threshold configuration."""

    # Process memory thresholds (MB)
    process_warning: float
    process_critical: float
    process_emergency: float

    # System memory thresholds (%)
    system_warning: float
    system_critical: float
    system_emergency: float

    # Memory growth rate thresholds (MB/minute)
    growth_rate_warning: float
    growth_rate_critical: float

    # Garbage collection thresholds
    gc_pressure_threshold: float
    gc_frequency_max: int


@dataclass
class PrometheusConfig:
    """Prometheus monitoring configuration."""

    enabled: bool = True
    port: int = 9090
    host: str = "0.0.0.0"
    metrics_path: str = "/metrics"
    push_gateway_url: Optional[str] = None
    job_name: str = "memory_monitor"


@dataclass
class AlertingConfig:
    """Alerting configuration."""

    enabled: bool = True
    webhook_url: Optional[str] = None
    email_enabled: bool = False
    email_recipients: List[str] = field(default_factory=list)
    slack_webhook: Optional[str] = None
    pagerduty_key: Optional[str] = None

    # Alert thresholds
    alert_cooldown_minutes: int = 5
    max_alerts_per_hour: int = 20
    escalation_timeout_minutes: int = 15


class MemoryMonitoringSettings(BaseSettings):
    """
    Memory monitoring settings with environment-based configuration.
    """

    # Environment configuration
    environment: MonitoringEnvironment = Field(
        default=MonitoringEnvironment.DEVELOPMENT,
        env="MEMORY_MONITOR_ENVIRONMENT"
    )

    # Basic monitoring settings
    enabled: bool = Field(default=True, env="MEMORY_MONITOR_ENABLED")
    monitoring_interval: float = Field(default=10.0, env="MEMORY_MONITOR_INTERVAL")
    history_retention_hours: int = Field(default=24, env="MEMORY_MONITOR_RETENTION_HOURS")

    # Optimization settings
    optimization_mode: OptimizationMode = Field(
        default=OptimizationMode.AUTOMATIC,
        env="MEMORY_OPTIMIZATION_MODE"
    )
    optimization_strategy: str = Field(default="balanced", env="MEMORY_OPTIMIZATION_STRATEGY")

    # Tracemalloc settings
    enable_tracemalloc: bool = Field(default=True, env="MEMORY_TRACEMALLOC_ENABLED")
    tracemalloc_nframe: int = Field(default=10, env="MEMORY_TRACEMALLOC_NFRAME")

    # Prometheus settings
    prometheus_enabled: bool = Field(default=True, env="PROMETHEUS_ENABLED")
    prometheus_port: int = Field(default=9090, env="PROMETHEUS_PORT")
    prometheus_host: str = Field(default="0.0.0.0", env="PROMETHEUS_HOST")

    # Alerting settings
    alerting_enabled: bool = Field(default=True, env="MEMORY_ALERTING_ENABLED")
    webhook_url: Optional[str] = Field(default=None, env="MEMORY_ALERT_WEBHOOK_URL")
    slack_webhook: Optional[str] = Field(default=None, env="MEMORY_ALERT_SLACK_WEBHOOK")

    # Logging settings
    log_level: str = Field(default="INFO", env="MEMORY_MONITOR_LOG_LEVEL")
    log_file: Optional[str] = Field(default=None, env="MEMORY_MONITOR_LOG_FILE")

    # Advanced settings
    leak_detection_enabled: bool = Field(default=True, env="MEMORY_LEAK_DETECTION_ENABLED")
    leak_detection_window_minutes: int = Field(default=60, env="MEMORY_LEAK_DETECTION_WINDOW")
    auto_optimization_enabled: bool = Field(default=True, env="MEMORY_AUTO_OPTIMIZATION_ENABLED")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False

    @validator("monitoring_interval")
    def validate_monitoring_interval(cls, v):
        if v < 1.0:
            raise ValueError("Monitoring interval must be at least 1 second")
        if v > 300.0:  # 5 minutes
            raise ValueError("Monitoring interval should not exceed 5 minutes")
        return v

    @validator("prometheus_port")
    def validate_prometheus_port(cls, v):
        if v < 1024 or v > 65535:
            raise ValueError("Prometheus port must be between 1024 and 65535")
        return v

    def get_memory_thresholds(self) -> MemoryThresholdConfig:
        """Get environment-specific memory thresholds."""

        if self.environment == MonitoringEnvironment.DEVELOPMENT:
            return MemoryThresholdConfig(
                process_warning=200.0,
                process_critical=500.0,
                process_emergency=1000.0,
                system_warning=70.0,
                system_critical=85.0,
                system_emergency=95.0,
                growth_rate_warning=5.0,
                growth_rate_critical=20.0,
                gc_pressure_threshold=70.0,
                gc_frequency_max=15
            )
        elif self.environment == MonitoringEnvironment.TESTING:
            return MemoryThresholdConfig(
                process_warning=100.0,
                process_critical=250.0,
                process_emergency=500.0,
                system_warning=60.0,
                system_critical=75.0,
                system_emergency=90.0,
                growth_rate_warning=3.0,
                growth_rate_critical=10.0,
                gc_pressure_threshold=60.0,
                gc_frequency_max=20
            )
        elif self.environment == MonitoringEnvironment.STAGING:
            return MemoryThresholdConfig(
                process_warning=500.0,
                process_critical=1000.0,
                process_emergency=2000.0,
                system_warning=75.0,
                system_critical=85.0,
                system_emergency=95.0,
                growth_rate_warning=10.0,
                growth_rate_critical=50.0,
                gc_pressure_threshold=75.0,
                gc_frequency_max=12
            )
        else:  # PRODUCTION
            return MemoryThresholdConfig(
                process_warning=1000.0,
                process_critical=2000.0,
                process_emergency=4000.0,
                system_warning=80.0,
                system_critical=90.0,
                system_emergency=97.0,
                growth_rate_warning=20.0,
                growth_rate_critical=100.0,
                gc_pressure_threshold=80.0,
                gc_frequency_max=10
            )

    def get_prometheus_config(self) -> PrometheusConfig:
        """Get Prometheus configuration."""

        return PrometheusConfig(
            enabled=self.prometheus_enabled,
            port=self.prometheus_port,
            host=self.prometheus_host,
            push_gateway_url=os.getenv("PROMETHEUS_PUSH_GATEWAY_URL"),
            job_name=f"memory_monitor_{self.environment.value}"
        )

    def get_alerting_config(self) -> AlertingConfig:
        """Get alerting configuration."""

        return AlertingConfig(
            enabled=self.alerting_enabled,
            webhook_url=self.webhook_url,
            slack_webhook=self.slack_webhook,
            email_enabled=os.getenv("MEMORY_ALERT_EMAIL_ENABLED", "false").lower() == "true",
            email_recipients=os.getenv("MEMORY_ALERT_EMAIL_RECIPIENTS", "").split(","),
            pagerduty_key=os.getenv("MEMORY_ALERT_PAGERDUTY_KEY"),
            alert_cooldown_minutes=int(os.getenv("MEMORY_ALERT_COOLDOWN_MINUTES", "5")),
            max_alerts_per_hour=int(os.getenv("MEMORY_ALERT_MAX_PER_HOUR", "20")),
            escalation_timeout_minutes=int(os.getenv("MEMORY_ALERT_ESCALATION_TIMEOUT", "15"))
        )


def get_memory_monitoring_config() -> MemoryMonitoringSettings:
    """Get memory monitoring configuration."""
    return MemoryMonitoringSettings()


def create_environment_config(
    environment: MonitoringEnvironment,
    config_dir: Optional[Path] = None
) -> Dict[str, str]:
    """Create environment-specific configuration."""

    if config_dir is None:
        config_dir = Path.cwd() / "config"

    config_dir.mkdir(exist_ok=True)

    base_config = {
        "MEMORY_MONITOR_ENVIRONMENT": environment.value,
        "MEMORY_MONITOR_ENABLED": "true",
        "MEMORY_TRACEMALLOC_ENABLED": "true",
        "PROMETHEUS_ENABLED": "true",
        "MEMORY_ALERTING_ENABLED": "true",
        "MEMORY_LEAK_DETECTION_ENABLED": "true",
        "MEMORY_AUTO_OPTIMIZATION_ENABLED": "true",
        "MEMORY_MONITOR_LOG_LEVEL": "INFO"
    }

    # Environment-specific overrides
    if environment == MonitoringEnvironment.DEVELOPMENT:
        base_config.update({
            "MEMORY_MONITOR_INTERVAL": "5.0",
            "MEMORY_OPTIMIZATION_STRATEGY": "conservative",
            "PROMETHEUS_PORT": "9091",
            "MEMORY_MONITOR_LOG_LEVEL": "DEBUG"
        })
    elif environment == MonitoringEnvironment.TESTING:
        base_config.update({
            "MEMORY_MONITOR_INTERVAL": "2.0",
            "MEMORY_OPTIMIZATION_STRATEGY": "aggressive",
            "PROMETHEUS_PORT": "9092",
            "MEMORY_ALERTING_ENABLED": "false"
        })
    elif environment == MonitoringEnvironment.STAGING:
        base_config.update({
            "MEMORY_MONITOR_INTERVAL": "10.0",
            "MEMORY_OPTIMIZATION_STRATEGY": "balanced",
            "PROMETHEUS_PORT": "9093"
        })
    else:  # PRODUCTION
        base_config.update({
            "MEMORY_MONITOR_INTERVAL": "30.0",
            "MEMORY_OPTIMIZATION_STRATEGY": "balanced",
            "PROMETHEUS_PORT": "9090",
            "MEMORY_MONITOR_RETENTION_HOURS": "72"
        })

    # Write configuration to file
    config_file = config_dir / f".env.{environment.value}"
    with open(config_file, "w") as f:
        for key, value in base_config.items():
            f.write(f"{key}={value}\n")

    return base_config


# Pre-configured settings for different environments
DEVELOPMENT_CONFIG = MemoryMonitoringSettings(
    environment=MonitoringEnvironment.DEVELOPMENT,
    monitoring_interval=5.0,
    optimization_strategy="conservative",
    prometheus_port=9091,
    log_level="DEBUG"
)

TESTING_CONFIG = MemoryMonitoringSettings(
    environment=MonitoringEnvironment.TESTING,
    monitoring_interval=2.0,
    optimization_strategy="aggressive",
    prometheus_port=9092,
    alerting_enabled=False
)

STAGING_CONFIG = MemoryMonitoringSettings(
    environment=MonitoringEnvironment.STAGING,
    monitoring_interval=10.0,
    optimization_strategy="balanced",
    prometheus_port=9093
)

PRODUCTION_CONFIG = MemoryMonitoringSettings(
    environment=MonitoringEnvironment.PRODUCTION,
    monitoring_interval=30.0,
    optimization_strategy="balanced",
    prometheus_port=9090,
    history_retention_hours=72
)


if __name__ == "__main__":
    # Example usage
    import json

    # Load configuration
    config = get_memory_monitoring_config()
    print(f"Memory monitoring configuration:")
    print(f"Environment: {config.environment}")
    print(f"Monitoring interval: {config.monitoring_interval}s")
    print(f"Optimization mode: {config.optimization_mode}")

    # Get environment-specific thresholds
    thresholds = config.get_memory_thresholds()
    print(f"\nMemory thresholds:")
    print(f"Process warning: {thresholds.process_warning}MB")
    print(f"Process critical: {thresholds.process_critical}MB")
    print(f"System warning: {thresholds.system_warning}%")

    # Create environment configs
    for env in MonitoringEnvironment:
        env_config = create_environment_config(env)
        print(f"\n{env.value.upper()} configuration created with {len(env_config)} settings")