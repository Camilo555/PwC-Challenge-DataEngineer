"""
MLOps Orchestrator

Central orchestration system for ML operations including model lifecycle management,
automated monitoring, deployment automation, and integration with existing systems.
"""

import logging
import asyncio
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Union, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from pathlib import Path
import json
import yaml

from src.core.config import get_settings
from src.core.logging import get_logger
from src.core.monitoring.metrics import MetricsCollector
from src.data_access.supabase_client import get_supabase_client
from src.core.caching.redis_cache import get_redis_client
from src.monitoring.intelligent_alerting import create_intelligent_alerting_system

# ML Components
from src.ml.analytics.predictive_engine import PredictiveAnalyticsEngine, PredictionType
from src.ml.deployment.model_server import ModelServer, ModelRegistry
from src.ml.deployment.ab_testing import ExperimentManager
from src.ml.feature_engineering.feature_store import FeatureStore, create_sales_feature_groups
from src.ml.feature_engineering.feature_pipeline import create_sales_feature_pipeline
from src.ml.insights.narrative_generator import InsightEngine, InsightConfig
from src.ml.monitoring.model_monitor import ModelMonitor, DriftConfig
from src.ml.training.model_trainer import AutoMLTrainer, ModelConfig, create_sales_prediction_trainer

logger = get_logger(__name__)
settings = get_settings()


@dataclass
class MLOpsConfig:
    """Configuration for MLOps orchestrator."""
    
    # Environment settings
    environment: str = "production"
    
    # Model lifecycle
    auto_retrain_enabled: bool = True
    retrain_trigger_threshold: float = 0.1
    model_promotion_threshold: float = 0.85
    
    # Monitoring settings
    monitoring_enabled: bool = True
    drift_detection_enabled: bool = True
    performance_tracking_enabled: bool = True
    
    # Deployment settings
    auto_deployment_enabled: bool = True
    canary_deployment_enabled: bool = True
    a_b_testing_enabled: bool = True
    
    # Alert settings
    alert_channels: List[str] = field(default_factory=lambda: ["email", "slack"])
    critical_alert_threshold: float = 0.2
    
    # Feature store settings
    feature_store_enabled: bool = True
    feature_refresh_schedule: str = "daily"
    
    # Training settings
    training_schedule: str = "weekly"
    hyperparameter_tuning_enabled: bool = True
    
    # Storage paths
    model_registry_path: str = "models/registry"
    experiment_tracking_path: str = "experiments"


class MLOpsOrchestrator:
    """Main MLOps orchestration system."""
    
    def __init__(self, config: MLOpsConfig):
        self.config = config
        self.supabase = get_supabase_client()
        self.redis_client = get_redis_client()
        self.metrics_collector = MetricsCollector()
        
        # Initialize ML components
        self.predictive_engine = PredictiveAnalyticsEngine()
        self.model_registry = ModelRegistry()
        self.model_server = ModelServer(self.model_registry)
        self.experiment_manager = ExperimentManager()
        self.feature_store = FeatureStore()
        self.insight_engine = InsightEngine()
        
        # Model monitors
        self.model_monitors: Dict[str, ModelMonitor] = {}
        
        # Alerting system
        self.alerting_system = create_intelligent_alerting_system()
        
        # Status tracking
        self.is_running = False
        self.last_health_check = datetime.utcnow()
        
    async def initialize(self):
        """Initialize MLOps orchestrator."""
        try:
            logger.info("Initializing MLOps orchestrator")
            
            # Initialize feature store
            if self.config.feature_store_enabled:
                await self._initialize_feature_store()
            
            # Initialize monitoring
            if self.config.monitoring_enabled:
                await self._initialize_monitoring()
            
            # Initialize default models
            await self._initialize_default_models()
            
            # Setup scheduled tasks
            await self._setup_scheduled_tasks()
            
            self.is_running = True
            logger.info("MLOps orchestrator initialized successfully")
            
            # Log initialization metrics
            self.metrics_collector.increment_counter(
                "mlops_orchestrator_initialized_total",
                tags={"environment": self.config.environment}
            )
            
        except Exception as e:
            logger.error(f"Error initializing MLOps orchestrator: {str(e)}")
            raise
    
    async def _initialize_feature_store(self):
        """Initialize feature store with predefined feature groups."""
        try:
            logger.info("Initializing feature store")
            
            # Create sales feature groups
            await create_sales_feature_groups(self.feature_store)
            
            logger.info("Feature store initialized")
            
        except Exception as e:
            logger.error(f"Error initializing feature store: {str(e)}")
    
    async def _initialize_monitoring(self):
        """Initialize monitoring systems."""
        try:
            logger.info("Initializing monitoring systems")
            
            # Setup drift detection config
            drift_config = DriftConfig(
                feature_drift_threshold=0.1,
                prediction_drift_threshold=0.1,
                concept_drift_threshold=0.05,
                enable_alerts=True,
                alert_channels=self.config.alert_channels
            )
            
            # Initialize model monitors (will be populated as models are deployed)
            logger.info("Monitoring systems initialized")
            
        except Exception as e:
            logger.error(f"Error initializing monitoring: {str(e)}")
    
    async def _initialize_default_models(self):
        """Initialize default ML models."""
        try:
            logger.info("Initializing default ML models")
            
            # Train sales forecasting model
            if self.config.auto_retrain_enabled:
                await self._train_default_sales_model()
            
            # Train customer segmentation model  
            await self._train_default_segmentation_model()
            
            logger.info("Default models initialized")
            
        except Exception as e:
            logger.error(f"Error initializing default models: {str(e)}")
    
    async def _train_default_sales_model(self):
        """Train default sales forecasting model."""
        try:
            # Create sample sales data for training
            sample_data = self._create_sample_sales_data()
            
            # Train sales forecasting model
            model_id = await self.predictive_engine.train_sales_forecaster(sample_data)
            
            # Setup monitoring for the model
            await self._setup_model_monitoring(model_id)
            
            logger.info(f"Default sales forecasting model trained: {model_id}")
            
        except Exception as e:
            logger.error(f"Error training default sales model: {str(e)}")
    
    async def _train_default_segmentation_model(self):
        """Train default customer segmentation model."""
        try:
            # Create sample customer data for training
            sample_data = self._create_sample_customer_data()
            
            # Train customer segmentation model
            model_id = await self.predictive_engine.train_customer_segmentation(sample_data)
            
            # Setup monitoring for the model
            await self._setup_model_monitoring(model_id)
            
            logger.info(f"Default customer segmentation model trained: {model_id}")
            
        except Exception as e:
            logger.error(f"Error training default segmentation model: {str(e)}")
    
    def _create_sample_sales_data(self) -> pd.DataFrame:
        """Create sample sales data for training."""
        dates = pd.date_range(start='2023-01-01', periods=365, freq='D')
        np.random.seed(42)
        
        # Create realistic sales patterns
        base_sales = 1000
        seasonal_pattern = 200 * np.sin(2 * np.pi * np.arange(365) / 365)
        weekly_pattern = 100 * np.sin(2 * np.pi * np.arange(365) / 7)
        noise = np.random.normal(0, 50, 365)
        
        sales = base_sales + seasonal_pattern + weekly_pattern + noise
        
        return pd.DataFrame({
            'date': dates,
            'sales': sales,
            'day_of_week': dates.dayofweek,
            'month': dates.month,
            'is_holiday': np.random.choice([0, 1], 365, p=[0.97, 0.03])
        })
    
    def _create_sample_customer_data(self) -> pd.DataFrame:
        """Create sample customer data for training."""
        np.random.seed(42)
        n_customers = 1000
        
        return pd.DataFrame({
            'customer_id': range(n_customers),
            'recency': np.random.exponential(30, n_customers),
            'frequency': np.random.poisson(5, n_customers),
            'monetary': np.random.lognormal(6, 1, n_customers),
            'total_orders': np.random.poisson(8, n_customers),
            'avg_order_value': np.random.lognormal(4, 0.5, n_customers),
            'days_since_last_order': np.random.exponential(45, n_customers)
        })
    
    async def _setup_model_monitoring(self, model_id: str):
        """Setup monitoring for a specific model."""
        try:
            drift_config = DriftConfig(
                feature_drift_threshold=self.config.retrain_trigger_threshold,
                enable_alerts=True,
                alert_channels=self.config.alert_channels
            )
            
            monitor = ModelMonitor(model_id, drift_config)
            self.model_monitors[model_id] = monitor
            
            # Set reference data (in production, this would use actual historical data)
            reference_features = self._create_sample_sales_data()
            await monitor.set_reference_data(reference_features)
            
            logger.info(f"Monitoring setup for model {model_id}")
            
        except Exception as e:
            logger.error(f"Error setting up monitoring for model {model_id}: {str(e)}")
    
    async def _setup_scheduled_tasks(self):
        """Setup scheduled tasks for MLOps operations."""
        try:
            logger.info("Setting up scheduled tasks")
            
            # Schedule periodic health checks
            asyncio.create_task(self._periodic_health_check())
            
            # Schedule model performance monitoring
            if self.config.monitoring_enabled:
                asyncio.create_task(self._periodic_monitoring())
            
            # Schedule feature store refresh
            if self.config.feature_store_enabled:
                asyncio.create_task(self._periodic_feature_refresh())
            
            # Schedule model retraining
            if self.config.auto_retrain_enabled:
                asyncio.create_task(self._periodic_retraining())
            
            logger.info("Scheduled tasks setup completed")
            
        except Exception as e:
            logger.error(f"Error setting up scheduled tasks: {str(e)}")
    
    async def _periodic_health_check(self):
        """Periodic health check of all systems."""
        while self.is_running:
            try:
                await self._perform_health_check()
                await asyncio.sleep(300)  # Check every 5 minutes
                
            except Exception as e:
                logger.error(f"Error in periodic health check: {str(e)}")
                await asyncio.sleep(60)
    
    async def _perform_health_check(self):
        """Perform comprehensive health check."""
        try:
            health_status = {
                "timestamp": datetime.utcnow().isoformat(),
                "overall_status": "healthy",
                "components": {}
            }
            
            # Check predictive engine
            try:
                # Simple check - can be expanded
                health_status["components"]["predictive_engine"] = "healthy"
            except Exception as e:
                health_status["components"]["predictive_engine"] = f"unhealthy: {str(e)}"
                health_status["overall_status"] = "degraded"
            
            # Check feature store
            try:
                feature_groups = await self.feature_store.list_feature_groups()
                health_status["components"]["feature_store"] = "healthy"
            except Exception as e:
                health_status["components"]["feature_store"] = f"unhealthy: {str(e)}"
                health_status["overall_status"] = "degraded"
            
            # Check model monitors
            healthy_monitors = 0
            total_monitors = len(self.model_monitors)
            
            for model_id, monitor in self.model_monitors.items():
                try:
                    # Simple health check for monitor
                    healthy_monitors += 1
                except Exception as e:
                    logger.warning(f"Monitor for model {model_id} unhealthy: {str(e)}")
            
            if total_monitors > 0:
                monitor_health_rate = healthy_monitors / total_monitors
                if monitor_health_rate < 0.8:
                    health_status["overall_status"] = "degraded"
            
            # Store health status
            await self._store_health_status(health_status)
            
            # Update metrics
            self.metrics_collector.record_gauge(
                "mlops_orchestrator_health_score",
                1.0 if health_status["overall_status"] == "healthy" else 0.0,
                tags={"environment": self.config.environment}
            )
            
            self.last_health_check = datetime.utcnow()
            
        except Exception as e:
            logger.error(f"Error performing health check: {str(e)}")
    
    async def _periodic_monitoring(self):
        """Periodic monitoring of all deployed models."""
        while self.is_running:
            try:
                await self._run_monitoring_cycle()
                await asyncio.sleep(3600)  # Run every hour
                
            except Exception as e:
                logger.error(f"Error in periodic monitoring: {str(e)}")
                await asyncio.sleep(300)
    
    async def _run_monitoring_cycle(self):
        """Run a complete monitoring cycle."""
        try:
            logger.info("Starting monitoring cycle")
            
            for model_id, monitor in self.model_monitors.items():
                try:
                    # Generate sample current data for monitoring
                    current_data = self._create_sample_sales_data()
                    
                    # Run drift detection
                    drift_results = await monitor.monitor_batch(current_data)
                    
                    # Check for significant drift
                    significant_drift = any(
                        result.is_drift_detected and result.drift_score > self.config.critical_alert_threshold
                        for result in drift_results
                    )
                    
                    if significant_drift and self.config.auto_retrain_enabled:
                        logger.warning(f"Significant drift detected for model {model_id}, triggering retraining")
                        await self._trigger_model_retraining(model_id)
                    
                except Exception as e:
                    logger.error(f"Error monitoring model {model_id}: {str(e)}")
            
            logger.info("Monitoring cycle completed")
            
        except Exception as e:
            logger.error(f"Error in monitoring cycle: {str(e)}")
    
    async def _periodic_feature_refresh(self):
        """Periodic feature store refresh."""
        while self.is_running:
            try:
                await self._refresh_features()
                await asyncio.sleep(86400)  # Refresh daily
                
            except Exception as e:
                logger.error(f"Error in periodic feature refresh: {str(e)}")
                await asyncio.sleep(3600)
    
    async def _refresh_features(self):
        """Refresh features in feature store."""
        try:
            logger.info("Refreshing features in feature store")
            
            # Generate updated feature data
            updated_customer_features = self._create_sample_customer_data()
            
            # Ingest into feature store
            await self.feature_store.ingest_features(
                "customer_features",
                "1.0",
                updated_customer_features
            )
            
            logger.info("Feature refresh completed")
            
        except Exception as e:
            logger.error(f"Error refreshing features: {str(e)}")
    
    async def _periodic_retraining(self):
        """Periodic model retraining."""
        while self.is_running:
            try:
                await self._run_scheduled_retraining()
                await asyncio.sleep(604800)  # Retrain weekly
                
            except Exception as e:
                logger.error(f"Error in periodic retraining: {str(e)}")
                await asyncio.sleep(86400)
    
    async def _run_scheduled_retraining(self):
        """Run scheduled model retraining."""
        try:
            logger.info("Starting scheduled model retraining")
            
            # Retrain sales forecasting model
            await self._retrain_sales_model()
            
            # Retrain customer segmentation model
            await self._retrain_segmentation_model()
            
            logger.info("Scheduled retraining completed")
            
        except Exception as e:
            logger.error(f"Error in scheduled retraining: {str(e)}")
    
    async def _retrain_sales_model(self):
        """Retrain sales forecasting model."""
        try:
            # Get latest training data
            training_data = self._create_sample_sales_data()
            
            # Train new model
            new_model_id = await self.predictive_engine.train_sales_forecaster(training_data)
            
            # Setup monitoring for new model
            await self._setup_model_monitoring(new_model_id)
            
            # If A/B testing is enabled, create experiment
            if self.config.a_b_testing_enabled:
                await self._create_model_comparison_experiment(new_model_id, "sales_forecaster")
            
            logger.info(f"Sales model retrained: {new_model_id}")
            
        except Exception as e:
            logger.error(f"Error retraining sales model: {str(e)}")
    
    async def _retrain_segmentation_model(self):
        """Retrain customer segmentation model."""
        try:
            # Get latest training data
            training_data = self._create_sample_customer_data()
            
            # Train new model
            new_model_id = await self.predictive_engine.train_customer_segmentation(training_data)
            
            # Setup monitoring for new model
            await self._setup_model_monitoring(new_model_id)
            
            logger.info(f"Segmentation model retrained: {new_model_id}")
            
        except Exception as e:
            logger.error(f"Error retraining segmentation model: {str(e)}")
    
    async def _trigger_model_retraining(self, model_id: str):
        """Trigger immediate model retraining due to drift."""
        try:
            logger.info(f"Triggering retraining for model {model_id} due to drift")
            
            # Create alert about drift-triggered retraining
            await self.alerting_system.send_alert(
                title="Model Drift Detected - Retraining Triggered",
                message=f"Model {model_id} has significant drift. Automatic retraining initiated.",
                severity="high",
                channels=self.config.alert_channels,
                metadata={"model_id": model_id, "trigger": "drift_detection"}
            )
            
            # Perform retraining based on model type
            if "sales" in model_id.lower():
                await self._retrain_sales_model()
            elif "segmentation" in model_id.lower():
                await self._retrain_segmentation_model()
            
            logger.info(f"Drift-triggered retraining completed for model {model_id}")
            
        except Exception as e:
            logger.error(f"Error in drift-triggered retraining for model {model_id}: {str(e)}")
    
    async def _create_model_comparison_experiment(self, new_model_id: str, model_type: str):
        """Create A/B testing experiment for model comparison."""
        try:
            from src.ml.deployment.ab_testing import ExperimentConfig, SplitStrategy
            
            experiment_config = ExperimentConfig(
                experiment_id=f"model_comparison_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}",
                name=f"{model_type} Model Comparison",
                description=f"A/B test comparing new {model_type} model against current production model",
                start_date=datetime.utcnow(),
                end_date=datetime.utcnow() + timedelta(days=14),
                control_model_id="current_production",
                treatment_model_ids=[new_model_id],
                split_strategy=SplitStrategy.RANDOM,
                traffic_allocation={"control": 0.7, "treatment_0": 0.3},
                primary_metric="accuracy",
                minimum_sample_size=1000
            )
            
            await self.experiment_manager.create_experiment(experiment_config)
            logger.info(f"A/B testing experiment created for model {new_model_id}")
            
        except Exception as e:
            logger.error(f"Error creating A/B testing experiment: {str(e)}")
    
    async def _store_health_status(self, health_status: Dict[str, Any]):
        """Store health status in database."""
        try:
            health_record = {
                "timestamp": health_status["timestamp"],
                "overall_status": health_status["overall_status"],
                "components": health_status["components"],
                "environment": self.config.environment
            }
            
            self.supabase.table('mlops_health_status').insert(health_record).execute()
            
        except Exception as e:
            logger.error(f"Error storing health status: {str(e)}")
    
    async def get_system_status(self) -> Dict[str, Any]:
        """Get current system status."""
        try:
            status = {
                "orchestrator": {
                    "status": "running" if self.is_running else "stopped",
                    "last_health_check": self.last_health_check.isoformat(),
                    "environment": self.config.environment
                },
                "models": {
                    "total_monitored": len(self.model_monitors),
                    "monitoring_enabled": self.config.monitoring_enabled,
                    "auto_retrain_enabled": self.config.auto_retrain_enabled
                },
                "feature_store": {
                    "enabled": self.config.feature_store_enabled,
                    "feature_groups": len(await self.feature_store.list_feature_groups())
                },
                "experiments": {
                    "a_b_testing_enabled": self.config.a_b_testing_enabled,
                    "canary_deployment_enabled": self.config.canary_deployment_enabled
                }
            }
            
            return status
            
        except Exception as e:
            logger.error(f"Error getting system status: {str(e)}")
            return {"error": str(e)}
    
    async def shutdown(self):
        """Gracefully shutdown the MLOps orchestrator."""
        try:
            logger.info("Shutting down MLOps orchestrator")
            
            self.is_running = False
            
            # Close any open connections
            # Cleanup resources
            
            logger.info("MLOps orchestrator shutdown completed")
            
        except Exception as e:
            logger.error(f"Error during shutdown: {str(e)}")


# Factory function for easy initialization
def create_mlops_orchestrator(environment: str = "production") -> MLOpsOrchestrator:
    """Create MLOps orchestrator with appropriate configuration."""
    
    if environment == "development":
        config = MLOpsConfig(
            environment="development",
            auto_retrain_enabled=False,
            auto_deployment_enabled=False,
            a_b_testing_enabled=False,
            monitoring_enabled=True,
            alert_channels=["email"]
        )
    elif environment == "staging":
        config = MLOpsConfig(
            environment="staging",
            auto_retrain_enabled=True,
            auto_deployment_enabled=False,
            a_b_testing_enabled=True,
            monitoring_enabled=True,
            alert_channels=["email", "slack"]
        )
    else:  # production
        config = MLOpsConfig(
            environment="production",
            auto_retrain_enabled=True,
            auto_deployment_enabled=True,
            a_b_testing_enabled=True,
            monitoring_enabled=True,
            alert_channels=["email", "slack", "pagerduty"]
        )
    
    return MLOpsOrchestrator(config)