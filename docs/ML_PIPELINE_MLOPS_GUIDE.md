# ML Pipeline and MLOps Guide

## Overview
This guide provides comprehensive documentation for the machine learning pipeline and MLOps practices implemented in the PwC Enterprise Data Engineering Platform.

## Table of Contents
1. [ML Pipeline Architecture](#ml-pipeline-architecture)
2. [Model Development Lifecycle](#model-development-lifecycle)
3. [Feature Engineering](#feature-engineering)
4. [Model Training and Validation](#model-training-and-validation)
5. [Model Deployment](#model-deployment)
6. [Model Monitoring and Maintenance](#model-monitoring-and-maintenance)
7. [MLOps Best Practices](#mlops-best-practices)
8. [Troubleshooting and Maintenance](#troubleshooting-and-maintenance)

## ML Pipeline Architecture

### High-Level Architecture
```mermaid
graph TB
    subgraph "Data Sources"
        RAW[Raw Data]
        STREAM[Streaming Data]
        EXTERNAL[External APIs]
    end
    
    subgraph "Feature Engineering"
        FE[Feature Pipeline]
        FS[Feature Store]
        VAL[Validation]
    end
    
    subgraph "Model Development"
        EXP[Experimentation]
        TRAIN[Training]
        EVAL[Evaluation]
    end
    
    subgraph "Model Registry"
        REG[Model Registry]
        VER[Versioning]
        META[Metadata]
    end
    
    subgraph "Deployment"
        STAGING[Staging]
        PROD[Production]
        AB[A/B Testing]
    end
    
    subgraph "Monitoring"
        PERF[Performance]
        DRIFT[Data Drift]
        ALERT[Alerting]
    end
    
    RAW --> FE
    STREAM --> FE
    EXTERNAL --> FE
    
    FE --> FS
    FS --> VAL
    VAL --> TRAIN
    
    TRAIN --> EXP
    EXP --> EVAL
    EVAL --> REG
    
    REG --> VER
    REG --> META
    REG --> STAGING
    
    STAGING --> PROD
    STAGING --> AB
    
    PROD --> PERF
    PROD --> DRIFT
    DRIFT --> ALERT
```

### Core Components

#### 1. Feature Engineering Pipeline
Located: `src/ml/feature_engineering/`

**Purpose**: Transform raw data into ML-ready features with proper validation and versioning.

**Key Features**:
- Automated feature extraction from transactional data
- Real-time feature computation for streaming data
- Feature validation and quality checks
- Feature versioning and lineage tracking

**Configuration Example**:
```python
from src.ml.feature_engineering.feature_pipeline import FeatureEngineeringPipeline

pipeline = FeatureEngineeringPipeline(
    config={
        "source_tables": ["bronze_sales", "dim_customers", "dim_products"],
        "feature_sets": ["customer_rfm", "product_popularity", "seasonal_trends"],
        "validation_rules": {
            "missing_threshold": 0.05,
            "outlier_detection": True,
            "schema_validation": True
        },
        "output_format": "parquet",
        "partitioning": ["year", "month"]
    }
)

# Execute pipeline
features = await pipeline.execute()
```

#### 2. Feature Store
Located: `src/ml/feature_engineering/feature_store.py`

**Purpose**: Centralized repository for ML features with online and offline access patterns.

**Features**:
- Online feature serving for real-time predictions
- Offline feature access for training
- Point-in-time correctness
- Feature versioning and rollback capability

**Usage Example**:
```python
from src.ml.feature_engineering.feature_store import FeatureStore

store = FeatureStore()

# Register feature set
store.register_feature_set(
    name="customer_features",
    features=["recency", "frequency", "monetary", "avg_order_value"],
    description="Customer behavioral features for segmentation"
)

# Get features for training
training_features = store.get_offline_features(
    feature_set="customer_features",
    entity_ids=["customer_123", "customer_456"],
    timestamp_range=("2024-01-01", "2024-12-31")
)

# Get features for real-time prediction
online_features = await store.get_online_features(
    feature_set="customer_features",
    entity_id="customer_123"
)
```

#### 3. Model Training Framework
Located: `src/ml/training/model_trainer.py`

**Purpose**: Automated model training with hyperparameter tuning and cross-validation.

**Supported Models**:
- Sales Forecasting: Prophet, ARIMA, XGBoost, LightGBM
- Customer Segmentation: KMeans, DBSCAN, Hierarchical Clustering
- Anomaly Detection: Isolation Forest, One-Class SVM, Autoencoders
- Demand Prediction: Random Forest, XGBoost, Neural Networks

**Training Configuration**:
```python
from src.ml.training.model_trainer import ModelTrainer

trainer = ModelTrainer(
    model_type="sales_forecast",
    algorithm="prophet",
    hyperparameters={
        "seasonality_mode": "multiplicative",
        "growth": "linear",
        "yearly_seasonality": True,
        "weekly_seasonality": True,
        "daily_seasonality": False
    },
    validation_strategy="time_series_split",
    n_splits=5
)

# Train model
model_result = await trainer.train(
    features=training_features,
    target="sales_amount",
    model_name="sales_forecast_v1.2"
)
```

## Model Development Lifecycle

### 1. Experimentation Phase

**Environment Setup**:
```bash
# Create experiment environment
cd src/ml/experiments/
jupyter lab --port=8888

# Install experiment dependencies
pip install -r requirements-experiment.txt
```

**Experiment Tracking**:
```python
import mlflow
from src.ml.training.model_trainer import ModelTrainer

# Start MLflow tracking
mlflow.set_experiment("sales_forecasting")

with mlflow.start_run():
    # Log parameters
    mlflow.log_params({
        "model_type": "prophet",
        "seasonality_mode": "multiplicative",
        "growth": "linear"
    })
    
    # Train model
    trainer = ModelTrainer(model_type="sales_forecast")
    model, metrics = await trainer.train(features, target)
    
    # Log metrics
    mlflow.log_metrics({
        "mape": metrics["mape"],
        "mae": metrics["mae"],
        "rmse": metrics["rmse"]
    })
    
    # Log model
    mlflow.sklearn.log_model(model, "model")
```

### 2. Model Validation

**Validation Framework**:
```python
from src.ml.training.model_trainer import ModelValidator

validator = ModelValidator()

# Comprehensive validation
validation_results = validator.validate_model(
    model=trained_model,
    test_data=test_features,
    validation_config={
        "performance_thresholds": {
            "mape": 0.15,  # Max 15% error
            "mae": 1000,   # Max 1000 units error
            "rmse": 1500   # Max 1500 units RMSE
        },
        "bias_detection": True,
        "drift_detection": True,
        "fairness_metrics": ["demographic_parity", "equalized_odds"]
    }
)

# Validation report
print(f"Validation Status: {validation_results.status}")
print(f"Performance Score: {validation_results.performance_score}")
print(f"Bias Score: {validation_results.bias_score}")
```

### 3. Model Registration

**Model Registry**:
```python
from src.ml.deployment.model_server import ModelRegistry

registry = ModelRegistry()

# Register model
model_version = registry.register_model(
    model_name="sales_forecast_prophet",
    model_version="1.2.0",
    model_artifact=model_artifact,
    metadata={
        "algorithm": "prophet",
        "training_data_version": "v2024.01",
        "features": ["date", "seasonality", "trend"],
        "performance_metrics": validation_results.metrics,
        "training_timestamp": datetime.now(),
        "author": "data_science_team"
    },
    tags=["production_ready", "sales_forecasting"]
)
```

## Feature Engineering

### Customer Features
```python
# Customer RFM Features
def create_customer_rfm_features(transactions_df):
    """Create Recency, Frequency, Monetary features for customers."""
    current_date = transactions_df['invoice_date'].max()
    
    customer_features = transactions_df.groupby('customer_id').agg({
        'invoice_date': lambda x: (current_date - x.max()).days,  # Recency
        'invoice_no': 'nunique',  # Frequency
        'total_amount': ['sum', 'mean', 'std', 'count']  # Monetary
    }).reset_index()
    
    customer_features.columns = [
        'customer_id', 'recency_days', 'frequency_orders',
        'monetary_total', 'monetary_avg', 'monetary_std', 'order_count'
    ]
    
    return customer_features

# Product Features
def create_product_features(transactions_df):
    """Create product popularity and seasonal features."""
    product_features = transactions_df.groupby(['stock_code', 'month']).agg({
        'quantity': ['sum', 'mean', 'std'],
        'unit_price': ['mean', 'std'],
        'total_amount': 'sum',
        'customer_id': 'nunique'
    }).reset_index()
    
    return product_features

# Time-based Features
def create_temporal_features(df):
    """Create time-based features for seasonality detection."""
    df['year'] = df['invoice_date'].dt.year
    df['month'] = df['invoice_date'].dt.month
    df['day_of_week'] = df['invoice_date'].dt.dayofweek
    df['quarter'] = df['invoice_date'].dt.quarter
    df['is_weekend'] = df['day_of_week'].isin([5, 6])
    df['is_holiday'] = df['invoice_date'].isin(get_holidays())
    
    return df
```

### Feature Validation
```python
from src.domain.validators.data_quality import DataQualityValidator

def validate_features(features_df):
    """Validate feature quality before training."""
    validator = DataQualityValidator()
    
    # Check for missing values
    missing_check = validator.check_missing_values(
        features_df, 
        threshold=0.05  # Max 5% missing values
    )
    
    # Check for outliers
    outlier_check = validator.detect_outliers(
        features_df,
        method="isolation_forest",
        contamination=0.1
    )
    
    # Schema validation
    schema_check = validator.validate_schema(
        features_df,
        expected_schema={
            'customer_id': 'string',
            'recency_days': 'integer',
            'frequency_orders': 'integer',
            'monetary_total': 'float'
        }
    )
    
    return {
        "missing_values": missing_check,
        "outliers": outlier_check,
        "schema": schema_check
    }
```

## Model Training and Validation

### Sales Forecasting Model
```python
from src.ml.analytics.predictive_engine import PredictiveEngine

async def train_sales_forecast_model():
    """Train and validate sales forecasting model."""
    
    engine = PredictiveEngine()
    
    # Prepare data
    sales_data = await engine.load_sales_data(
        start_date="2022-01-01",
        end_date="2024-01-01"
    )
    
    # Feature engineering
    features = engine.engineer_features(
        sales_data,
        feature_sets=["temporal", "seasonal", "trend", "external"]
    )
    
    # Train model
    model_config = {
        "algorithm": "prophet",
        "seasonality": {
            "yearly": True,
            "weekly": True,
            "daily": False
        },
        "holidays": True,
        "growth": "linear"
    }
    
    model = await engine.train_forecast_model(
        data=features,
        config=model_config,
        validation_method="time_series_split"
    )
    
    # Validate model
    validation_results = await engine.validate_model(
        model=model,
        test_data=test_features,
        metrics=["mape", "mae", "rmse", "smape"]
    )
    
    return model, validation_results

# Customer Segmentation Model
async def train_segmentation_model():
    """Train customer segmentation model."""
    
    engine = PredictiveEngine()
    
    # Load customer data
    customer_data = await engine.load_customer_data(
        feature_window="12M"
    )
    
    # Create RFM features
    rfm_features = engine.create_rfm_features(customer_data)
    
    # Train clustering model
    segmentation_config = {
        "algorithm": "kmeans",
        "n_clusters": 5,
        "features": ["recency", "frequency", "monetary"],
        "scaling": "standard",
        "random_state": 42
    }
    
    model = await engine.train_segmentation_model(
        features=rfm_features,
        config=segmentation_config
    )
    
    # Evaluate segmentation quality
    evaluation = engine.evaluate_segmentation(
        model=model,
        features=rfm_features,
        metrics=["silhouette_score", "calinski_harabasz_score"]
    )
    
    return model, evaluation
```

### Model Performance Monitoring
```python
from src.ml.monitoring.model_monitor import ModelMonitor

# Initialize monitoring
monitor = ModelMonitor(
    model_name="sales_forecast_prophet",
    model_version="1.2.0"
)

# Monitor model performance
performance_metrics = monitor.track_performance(
    predictions=model_predictions,
    actual_values=actual_sales,
    metrics=["mape", "mae", "rmse"]
)

# Data drift detection
drift_results = monitor.detect_data_drift(
    reference_data=training_features,
    current_data=production_features,
    threshold=0.05
)

# Alert on performance degradation
if performance_metrics["mape"] > 0.20:  # 20% threshold
    monitor.send_alert(
        alert_type="performance_degradation",
        severity="high",
        message=f"Model MAPE increased to {performance_metrics['mape']:.2%}"
    )
```

## Model Deployment

### Staging Deployment
```python
from src.ml.deployment.model_server import ModelServer

async def deploy_to_staging():
    """Deploy model to staging environment."""
    
    server = ModelServer(environment="staging")
    
    deployment_config = {
        "model_name": "sales_forecast_prophet",
        "model_version": "1.2.0",
        "replicas": 2,
        "resources": {
            "cpu": "1000m",
            "memory": "2Gi"
        },
        "auto_scaling": {
            "min_replicas": 1,
            "max_replicas": 5,
            "target_cpu": 70
        }
    }
    
    deployment = await server.deploy_model(
        config=deployment_config,
        health_checks=True,
        monitoring=True
    )
    
    return deployment
```

### Production Deployment
```python
async def deploy_to_production():
    """Deploy model to production with blue-green deployment."""
    
    server = ModelServer(environment="production")
    
    # Blue-green deployment
    deployment = await server.blue_green_deploy(
        model_name="sales_forecast_prophet",
        new_version="1.2.0",
        current_version="1.1.0",
        traffic_split={"blue": 90, "green": 10},  # Canary release
        rollback_threshold={
            "error_rate": 0.01,
            "latency_p95": 1000,
            "performance_drop": 0.05
        }
    )
    
    return deployment

# A/B Testing Deployment
async def deploy_ab_test():
    """Deploy competing models for A/B testing."""
    
    from src.ml.deployment.ab_testing import ABTestManager
    
    ab_manager = ABTestManager()
    
    ab_test = await ab_manager.create_test(
        test_name="prophet_vs_xgboost",
        models={
            "control": {
                "name": "sales_forecast_prophet",
                "version": "1.2.0",
                "traffic_percentage": 70
            },
            "treatment": {
                "name": "sales_forecast_xgboost",
                "version": "1.0.0",
                "traffic_percentage": 30
            }
        },
        success_metrics=["mape", "mae", "latency"],
        test_duration_days=14
    )
    
    return ab_test
```

### Model Serving API
```python
from fastapi import FastAPI, HTTPException
from src.ml.deployment.model_server import get_model_predictor

app = FastAPI()
predictor = get_model_predictor("sales_forecast_prophet", "1.2.0")

@app.post("/api/v1/ml/predict")
async def predict_sales(request: PredictionRequest):
    """Generate sales prediction."""
    
    try:
        # Validate input
        if not request.features:
            raise HTTPException(400, "Features required")
        
        # Generate prediction
        prediction = await predictor.predict(
            features=request.features,
            horizon=request.horizon,
            confidence_interval=0.95
        )
        
        return {
            "prediction": prediction.values,
            "confidence_lower": prediction.lower_bound,
            "confidence_upper": prediction.upper_bound,
            "model_version": "1.2.0",
            "prediction_timestamp": datetime.now()
        }
        
    except Exception as e:
        raise HTTPException(500, f"Prediction failed: {str(e)}")
```

## Model Monitoring and Maintenance

### Performance Monitoring Dashboard
```python
from src.monitoring.enterprise_dashboard import MLOpsDashboard

dashboard = MLOpsDashboard()

# Model performance metrics
performance_panel = dashboard.create_performance_panel(
    models=["sales_forecast_prophet", "customer_segmentation_kmeans"],
    metrics=["accuracy", "latency", "throughput"],
    time_window="7d"
)

# Data drift monitoring
drift_panel = dashboard.create_drift_panel(
    models=["sales_forecast_prophet"],
    features=["seasonality", "trend", "external_factors"],
    alert_threshold=0.05
)

# Model health overview
health_panel = dashboard.create_health_panel(
    models=["all"],
    components=["serving", "monitoring", "storage"],
    status_indicators=["healthy", "degraded", "unhealthy"]
)
```

### Automated Retraining Pipeline
```python
from src.ml.training.model_trainer import AutoRetrainingPipeline

# Configure automated retraining
retraining_pipeline = AutoRetrainingPipeline(
    model_name="sales_forecast_prophet",
    triggers={
        "schedule": "weekly",  # Regular retraining
        "performance_threshold": {"mape": 0.20},  # Performance-based trigger
        "data_drift_threshold": 0.05,  # Drift-based trigger
        "min_new_data_points": 1000  # Data volume trigger
    }
)

# Execute retraining
async def auto_retrain_check():
    """Check if retraining is needed and execute if necessary."""
    
    retrain_needed = await retraining_pipeline.check_retrain_conditions()
    
    if retrain_needed.should_retrain:
        logger.info(f"Retraining triggered: {retrain_needed.reason}")
        
        # Retrain model
        new_model = await retraining_pipeline.retrain(
            data_source="production",
            validation_strategy="holdout",
            auto_deploy_threshold={"mape": 0.15}
        )
        
        # Validate new model
        validation_results = await retraining_pipeline.validate_new_model(
            new_model=new_model,
            baseline_model=current_model,
            test_data=latest_test_data
        )
        
        # Auto-deploy if validation passes
        if validation_results.is_better:
            await retraining_pipeline.deploy_new_model(
                model=new_model,
                deployment_strategy="canary"
            )
```

## MLOps Best Practices

### 1. Version Control and Reproducibility
```yaml
# .mlflow/experiments/config.yaml
experiment_config:
  name: "sales_forecasting"
  tracking_uri: "postgresql://mlflow:password@mlflow-db:5432/mlflow"
  artifact_store: "s3://mlflow-artifacts/experiments/"
  
model_config:
  versioning_strategy: "semantic"  # major.minor.patch
  artifact_storage: "s3://models-artifacts/"
  metadata_storage: "postgresql://models-db:5432/model_registry"
  
reproducibility:
  seed: 42
  environment: "requirements.txt"
  docker_image: "mlops/training:v1.2.0"
  code_version: "git_commit_hash"
```

### 2. CI/CD Pipeline for ML
```yaml
# .github/workflows/ml-pipeline.yml
name: ML Pipeline

on:
  push:
    paths: 
      - 'src/ml/**'
  pull_request:
    paths:
      - 'src/ml/**'

jobs:
  test:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v2
      
      - name: Setup Python
        uses: actions/setup-python@v2
        with:
          python-version: '3.10'
          
      - name: Install dependencies
        run: |
          pip install -r requirements.txt
          pip install -r requirements-test.txt
          
      - name: Run unit tests
        run: pytest tests/ml/
        
      - name: Run integration tests
        run: pytest tests/integration/ml/
        
      - name: Validate model schema
        run: python scripts/validate_model_schema.py
        
  train:
    needs: test
    runs-on: ubuntu-latest
    if: github.event_name == 'push' && github.ref == 'refs/heads/main'
    steps:
      - name: Train model
        run: |
          python src/ml/training/train_model.py \
            --config configs/sales_forecast.yaml \
            --output-dir artifacts/models/
            
      - name: Validate model
        run: |
          python src/ml/validation/validate_model.py \
            --model-path artifacts/models/latest \
            --validation-data data/test/
            
  deploy:
    needs: train
    runs-on: ubuntu-latest
    steps:
      - name: Deploy to staging
        run: |
          python src/ml/deployment/deploy.py \
            --environment staging \
            --model-path artifacts/models/latest
            
      - name: Run deployment tests
        run: |
          python tests/deployment/test_model_serving.py \
            --endpoint https://staging-api.pwc-data.com
```

### 3. Data and Model Lineage
```python
from src.ml.lineage.lineage_tracker import LineageTracker

tracker = LineageTracker()

# Track data lineage
data_lineage = tracker.track_data_pipeline(
    pipeline_name="sales_feature_engineering",
    inputs=[
        "bronze_layer.sales_transactions",
        "silver_layer.dim_customers",
        "external_api.exchange_rates"
    ],
    outputs=[
        "feature_store.customer_rfm_features",
        "feature_store.product_features"
    ],
    transformations=[
        "rfm_calculation",
        "product_aggregation",
        "currency_normalization"
    ]
)

# Track model lineage
model_lineage = tracker.track_model_pipeline(
    model_name="sales_forecast_prophet",
    model_version="1.2.0",
    training_data="feature_store.sales_features_v2024_01",
    features_used=["date", "seasonality", "trend", "external_factors"],
    parent_models=None,  # Base model
    hyperparameters={"seasonality_mode": "multiplicative"},
    training_code_version="commit_abc123"
)
```

### 4. Model Governance
```python
from src.ml.governance.model_governance import ModelGovernance

governance = ModelGovernance()

# Model approval workflow
approval_request = governance.request_approval(
    model_name="sales_forecast_prophet",
    model_version="1.2.0",
    deployment_target="production",
    business_justification="Improved accuracy by 5% over current model",
    risk_assessment={
        "business_impact": "medium",
        "technical_risk": "low",
        "regulatory_compliance": "compliant"
    },
    approvers=["lead_data_scientist", "ml_engineer", "business_owner"]
)

# Compliance checks
compliance_report = governance.run_compliance_checks(
    model="sales_forecast_prophet",
    checks=[
        "bias_detection",
        "fairness_assessment",
        "explainability_requirements",
        "data_privacy_compliance"
    ]
)
```

## Troubleshooting and Maintenance

### Common Issues and Solutions

#### 1. Model Performance Degradation
```python
# Diagnostic script
async def diagnose_performance_issue(model_name: str):
    """Diagnose model performance issues."""
    
    from src.ml.monitoring.diagnostics import PerformanceDiagnostic
    
    diagnostic = PerformanceDiagnostic(model_name)
    
    # Check data drift
    drift_analysis = await diagnostic.analyze_data_drift(
        time_window="30d",
        features="all"
    )
    
    # Check concept drift
    concept_drift = await diagnostic.analyze_concept_drift(
        prediction_window="30d",
        actual_window="30d"
    )
    
    # Check model health
    model_health = await diagnostic.check_model_health(
        metrics=["accuracy", "latency", "memory_usage"]
    )
    
    # Generate recommendations
    recommendations = diagnostic.generate_recommendations([
        drift_analysis,
        concept_drift,
        model_health
    ])
    
    return {
        "data_drift": drift_analysis,
        "concept_drift": concept_drift,
        "model_health": model_health,
        "recommendations": recommendations
    }
```

#### 2. Training Pipeline Failures
```python
# Recovery script
async def recover_failed_training(job_id: str):
    """Recover from training pipeline failure."""
    
    from src.ml.training.recovery import TrainingRecovery
    
    recovery = TrainingRecovery(job_id)
    
    # Analyze failure
    failure_analysis = recovery.analyze_failure()
    
    # Determine recovery strategy
    if failure_analysis.failure_type == "data_corruption":
        # Restore from backup
        clean_data = recovery.restore_training_data(
            backup_date=failure_analysis.last_known_good
        )
        
    elif failure_analysis.failure_type == "resource_exhaustion":
        # Adjust resources and retry
        recovery.adjust_training_resources(
            memory_multiplier=1.5,
            cpu_multiplier=1.2
        )
        
    elif failure_analysis.failure_type == "hyperparameter_error":
        # Rollback to last known good config
        recovery.rollback_hyperparameters()
    
    # Restart training
    new_job = recovery.restart_training()
    
    return new_job
```

#### 3. Model Serving Issues
```python
# Serving diagnostics
async def diagnose_serving_issues(model_endpoint: str):
    """Diagnose model serving performance issues."""
    
    from src.ml.deployment.diagnostics import ServingDiagnostic
    
    diagnostic = ServingDiagnostic(model_endpoint)
    
    # Check endpoint health
    health_check = await diagnostic.check_endpoint_health()
    
    # Analyze latency issues
    latency_analysis = await diagnostic.analyze_latency(
        time_window="1h",
        percentiles=[50, 95, 99]
    )
    
    # Check resource utilization
    resource_usage = await diagnostic.check_resource_usage()
    
    # Test prediction quality
    prediction_test = await diagnostic.test_predictions(
        test_cases="standard_test_suite"
    )
    
    return {
        "health": health_check,
        "latency": latency_analysis,
        "resources": resource_usage,
        "predictions": prediction_test
    }
```

### Maintenance Procedures

#### 1. Regular Model Retraining
```bash
#!/bin/bash
# scripts/retrain_models.sh

# Weekly retraining schedule
echo "Starting weekly model retraining..."

# Check data freshness
python src/ml/maintenance/check_data_freshness.py

# Retrain sales forecasting model
python src/ml/training/train_model.py \
  --config configs/sales_forecast.yaml \
  --data-window 2y \
  --auto-deploy-if-better

# Retrain customer segmentation
python src/ml/training/train_model.py \
  --config configs/customer_segmentation.yaml \
  --data-window 1y \
  --validation-strategy k-fold

# Generate performance report
python src/ml/monitoring/generate_performance_report.py \
  --period weekly \
  --models all \
  --output reports/weekly_model_performance.html

echo "Weekly retraining completed."
```

#### 2. Model Cleanup
```python
# scripts/cleanup_models.py
async def cleanup_old_models():
    """Clean up old model versions and artifacts."""
    
    from src.ml.deployment.model_server import ModelRegistry
    
    registry = ModelRegistry()
    
    # Get all models
    models = registry.list_models()
    
    for model in models:
        # Keep only last 5 versions for non-production models
        if model.environment != "production":
            old_versions = registry.get_model_versions(
                model_name=model.name,
                limit=100,
                offset=5  # Skip latest 5
            )
            
            for version in old_versions:
                if version.created_at < datetime.now() - timedelta(days=30):
                    registry.delete_model_version(
                        model_name=model.name,
                        version=version.version
                    )
                    
        # Archive models not used in 90 days
        if model.last_used < datetime.now() - timedelta(days=90):
            registry.archive_model(model.name)

# Run cleanup
asyncio.run(cleanup_old_models())
```

### Support and Resources

#### Documentation Links
- [MLflow Model Registry](https://mlflow.org/docs/latest/model-registry.html)
- [Feast Feature Store](https://feast.dev/)
- [Evidently AI Model Monitoring](https://evidentlyai.com/)

#### Team Contacts
- **ML Engineering Team**: ml-engineering@pwc.com
- **Data Science Team**: data-science@pwc.com  
- **Platform Team**: platform-team@pwc.com

#### Emergency Procedures
1. **Model Serving Down**: Run `scripts/emergency_model_rollback.sh`
2. **Training Pipeline Failure**: Contact ML Engineering team
3. **Performance Degradation**: Execute performance diagnostic script

#### Monitoring Dashboards
- **MLOps Dashboard**: https://mlops-dashboard.pwc-data.com
- **Model Performance**: https://monitoring.pwc-data.com/ml-models
- **Training Pipeline Status**: https://training.pwc-data.com/status