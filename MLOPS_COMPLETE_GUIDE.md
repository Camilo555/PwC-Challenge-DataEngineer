# MLOps Complete Guide

## Table of Contents
1. [MLOps Overview](#mlops-overview)
2. [Architecture & Components](#architecture--components)
3. [ML Pipeline Management](#ml-pipeline-management)
4. [Model Development Lifecycle](#model-development-lifecycle)
5. [Feature Engineering & Store](#feature-engineering--store)
6. [Model Training & Validation](#model-training--validation)
7. [Model Deployment & Serving](#model-deployment--serving)
8. [Model Monitoring & Observability](#model-monitoring--observability)
9. [A/B Testing Framework](#ab-testing-framework)
10. [Data Drift Detection](#data-drift-detection)
11. [MLOps Automation](#mlops-automation)
12. [Security & Governance](#security--governance)
13. [Performance Optimization](#performance-optimization)
14. [Troubleshooting](#troubleshooting)

## MLOps Overview

### What is MLOps?
MLOps (Machine Learning Operations) is a practice that combines machine learning, DevOps, and data engineering to deploy and maintain ML systems in production reliably and efficiently.

### Key Principles
- **Automation**: Automate ML workflows from data ingestion to model deployment
- **Versioning**: Version control for data, code, and models
- **Monitoring**: Continuous monitoring of model performance and data quality
- **Reproducibility**: Ensure experiments and deployments are reproducible
- **Scalability**: Scale ML systems to handle production workloads
- **Governance**: Implement proper governance and compliance

### Platform Components
Our MLOps platform consists of:
- **MLOps Orchestrator**: Central coordination system
- **Feature Store**: Centralized feature management
- **Model Registry**: Version control for ML models
- **Training Pipeline**: Automated model training
- **Serving Infrastructure**: Model deployment and serving
- **Monitoring System**: Model and data monitoring
- **Experiment Tracking**: ML experiment management

## Architecture & Components

### High-Level Architecture
```
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Data Sources  │───▶│  Feature Store   │───▶│  ML Pipelines   │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                │                        │
                                ▼                        ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   Monitoring    │◀───│ MLOps Orchestr.  │───▶│ Model Registry  │
└─────────────────┘    └──────────────────┘    └─────────────────┘
                                │                        │
                                ▼                        ▼
┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐
│   A/B Testing   │◀───│  Model Serving   │◀───│   Deployment    │
└─────────────────┘    └──────────────────┘    └─────────────────┘
```

### Core Components

#### 1. MLOps Orchestrator
**File**: `src/ml/mlops_orchestrator.py`

**Purpose**: Central coordination system that manages the entire ML lifecycle

**Key Features**:
- Automated model lifecycle management
- Pipeline orchestration and scheduling
- Health monitoring and alerting
- Resource management
- Integration with all ML components

**Configuration**:
```python
from src.ml.mlops_orchestrator import MLOpsConfig, create_mlops_orchestrator

config = MLOpsConfig(
    environment="production",
    auto_retrain_enabled=True,
    monitoring_enabled=True,
    a_b_testing_enabled=True,
    feature_store_enabled=True
)

orchestrator = create_mlops_orchestrator("production")
await orchestrator.initialize()
```

#### 2. Predictive Analytics Engine
**File**: `src/ml/analytics/predictive_engine.py`

**Capabilities**:
- Sales forecasting with ARIMA and Prophet models
- Customer segmentation using K-means and DBSCAN
- Anomaly detection with Isolation Forest
- Real-time and batch prediction serving

**Usage Example**:
```python
from src.ml.analytics.predictive_engine import PredictiveAnalyticsEngine

engine = PredictiveAnalyticsEngine()

# Train a sales forecasting model
training_data = load_sales_data()
model_id = await engine.train_sales_forecaster(training_data)

# Make predictions
prediction = await engine.predict("sales_forecaster", input_data)
```

#### 3. Feature Store
**File**: `src/ml/feature_engineering/feature_store.py`

**Features**:
- Point-in-time feature retrieval
- Feature versioning and lineage
- Real-time and batch feature serving
- Data quality validation

**Feature Groups**:
```python
# Customer features
customer_features = {
    'recency': 'Days since last purchase',
    'frequency': 'Number of purchases',
    'monetary': 'Total purchase amount',
    'avg_order_value': 'Average order value',
    'customer_lifetime_value': 'Predicted CLV'
}

# Product features
product_features = {
    'category': 'Product category',
    'price': 'Product price',
    'rating': 'Average customer rating',
    'inventory_level': 'Current inventory'
}
```

## ML Pipeline Management

### Pipeline Architecture
Our ML pipelines follow a standardized structure:

```
Data Ingestion → Feature Engineering → Model Training → Validation → Deployment → Monitoring
```

### Pipeline Types

#### 1. Training Pipeline
**Purpose**: Automated model training and validation

**Stages**:
1. **Data Validation**: Check data quality and schema
2. **Feature Engineering**: Create and transform features
3. **Model Training**: Train ML models with hyperparameter tuning
4. **Model Validation**: Validate model performance
5. **Model Registration**: Register approved models

**Configuration**:
```python
training_config = {
    "data_source": "sales_data_warehouse",
    "feature_pipeline": "customer_features_v2",
    "model_type": "sales_forecasting",
    "validation_split": 0.2,
    "hyperparameter_tuning": True,
    "performance_threshold": 0.85
}
```

#### 2. Inference Pipeline
**Purpose**: Serve model predictions in production

**Components**:
- **Model Loading**: Load trained models from registry
- **Feature Retrieval**: Get features from feature store
- **Prediction**: Generate model predictions
- **Response Formatting**: Format and return results

#### 3. Batch Processing Pipeline
**Purpose**: Process large datasets for batch predictions

**Features**:
- Distributed processing with Spark
- Checkpointing for fault tolerance
- Progress monitoring and logging
- Result storage and notifications

### Pipeline Orchestration

#### Using Dagster
**File**: `src/orchestration/assets.py`

```python
from dagster import asset, AssetIn

@asset(ins={"sales_data": AssetIn("bronze_sales_data")})
def customer_features(sales_data):
    """Generate customer features from sales data."""
    return generate_customer_features(sales_data)

@asset(ins={"features": AssetIn("customer_features")})
def trained_model(features):
    """Train customer segmentation model."""
    return train_segmentation_model(features)
```

#### Using Airflow
**File**: `src/airflow_dags/enterprise_retail_etl_dag.py`

```python
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

dag = DAG('ml_training_pipeline', schedule_interval='@daily')

feature_engineering = PythonOperator(
    task_id='feature_engineering',
    python_callable=run_feature_engineering,
    dag=dag
)

model_training = PythonOperator(
    task_id='model_training',
    python_callable=train_models,
    dag=dag
)

feature_engineering >> model_training
```

## Model Development Lifecycle

### 1. Problem Definition
- Define business objectives
- Identify success metrics
- Determine data requirements
- Assess feasibility

### 2. Data Exploration
```python
# Exploratory Data Analysis
import pandas as pd
import seaborn as sns

def analyze_sales_data(df):
    """Comprehensive EDA for sales data."""
    
    # Basic statistics
    print(df.describe())
    
    # Data quality checks
    missing_data = df.isnull().sum()
    duplicates = df.duplicated().sum()
    
    # Temporal analysis
    df['date'] = pd.to_datetime(df['date'])
    monthly_sales = df.groupby(df['date'].dt.to_period('M'))['sales'].sum()
    
    # Customer analysis
    customer_stats = df.groupby('customer_id').agg({
        'sales': ['count', 'sum', 'mean'],
        'date': ['min', 'max']
    })
    
    return {
        'basic_stats': df.describe(),
        'missing_data': missing_data,
        'duplicates': duplicates,
        'monthly_sales': monthly_sales,
        'customer_stats': customer_stats
    }
```

### 3. Feature Engineering
**File**: `src/ml/feature_engineering/feature_pipeline.py`

```python
class SalesFeaturePipeline:
    """Feature engineering pipeline for sales data."""
    
    def __init__(self):
        self.transformers = {
            'temporal': self.create_temporal_features,
            'customer': self.create_customer_features,
            'product': self.create_product_features,
            'interaction': self.create_interaction_features
        }
    
    def create_temporal_features(self, df):
        """Create time-based features."""
        df['year'] = df['date'].dt.year
        df['month'] = df['date'].dt.month
        df['day_of_week'] = df['date'].dt.dayofweek
        df['is_weekend'] = df['day_of_week'].isin([5, 6])
        df['is_holiday'] = df['date'].isin(self.holiday_dates)
        
        # Seasonality features
        df['season'] = df['month'].map(self.month_to_season)
        
        return df
    
    def create_customer_features(self, df):
        """Create customer behavior features."""
        customer_features = df.groupby('customer_id').agg({
            'date': ['min', 'max', 'count'],
            'quantity': ['sum', 'mean'],
            'unit_price': ['mean', 'std'],
            'total_amount': ['sum', 'mean']
        }).round(2)
        
        # RFM features
        current_date = df['date'].max()
        customer_features['recency'] = (current_date - customer_features[('date', 'max')]).dt.days
        customer_features['frequency'] = customer_features[('date', 'count')]
        customer_features['monetary'] = customer_features[('total_amount', 'sum')]
        
        return customer_features
```

### 4. Model Training
**File**: `src/ml/training/model_trainer.py`

```python
from src.ml.training.model_trainer import AutoMLTrainer

class SalesForecaster:
    """Sales forecasting model trainer."""
    
    def __init__(self, config):
        self.config = config
        self.models = {
            'arima': self.train_arima,
            'prophet': self.train_prophet,
            'lstm': self.train_lstm
        }
    
    def train_arima(self, data):
        """Train ARIMA model for sales forecasting."""
        from statsmodels.tsa.arima.model import ARIMA
        
        # Prepare time series data
        ts_data = data.set_index('date')['sales'].resample('D').sum()
        
        # Grid search for optimal parameters
        best_aic = float('inf')
        best_params = None
        
        for p in range(3):
            for d in range(2):
                for q in range(3):
                    try:
                        model = ARIMA(ts_data, order=(p, d, q))
                        fitted_model = model.fit()
                        if fitted_model.aic < best_aic:
                            best_aic = fitted_model.aic
                            best_params = (p, d, q)
                    except:
                        continue
        
        # Train final model
        final_model = ARIMA(ts_data, order=best_params).fit()
        
        return {
            'model': final_model,
            'params': best_params,
            'aic': best_aic,
            'train_score': self.calculate_metrics(final_model, ts_data)
        }
```

### 5. Model Validation
```python
def validate_model_performance(model, test_data, metrics=['mae', 'rmse', 'mape']):
    """Comprehensive model validation."""
    
    predictions = model.predict(test_data)
    results = {}
    
    for metric in metrics:
        if metric == 'mae':
            results['mae'] = mean_absolute_error(test_data.target, predictions)
        elif metric == 'rmse':
            results['rmse'] = np.sqrt(mean_squared_error(test_data.target, predictions))
        elif metric == 'mape':
            results['mape'] = np.mean(np.abs((test_data.target - predictions) / test_data.target)) * 100
    
    # Business metrics
    results['forecast_accuracy'] = calculate_forecast_accuracy(test_data.target, predictions)
    results['directional_accuracy'] = calculate_directional_accuracy(test_data.target, predictions)
    
    return results
```

## Feature Engineering & Store

### Feature Store Architecture
```
Raw Data → Feature Engineering → Feature Store → Model Training/Inference
    │              │                  │               │
    └─────────── Validation ─────── Versioning ──── Monitoring
```

### Feature Categories

#### 1. Customer Features
```python
customer_feature_definitions = {
    'recency': {
        'description': 'Days since last purchase',
        'type': 'numeric',
        'aggregation': 'max',
        'source': 'transactions'
    },
    'frequency': {
        'description': 'Number of purchases in last 365 days',
        'type': 'numeric',
        'aggregation': 'count',
        'source': 'transactions'
    },
    'monetary': {
        'description': 'Total purchase amount in last 365 days',
        'type': 'numeric',
        'aggregation': 'sum',
        'source': 'transactions'
    },
    'avg_order_value': {
        'description': 'Average order value',
        'type': 'numeric',
        'aggregation': 'mean',
        'source': 'transactions'
    }
}
```

#### 2. Product Features
```python
product_feature_definitions = {
    'category_popularity': {
        'description': 'Product category popularity score',
        'type': 'numeric',
        'aggregation': 'mean',
        'source': 'product_analytics'
    },
    'price_percentile': {
        'description': 'Price percentile within category',
        'type': 'numeric',
        'aggregation': 'rank',
        'source': 'products'
    },
    'inventory_ratio': {
        'description': 'Current inventory to historical average ratio',
        'type': 'numeric',
        'aggregation': 'ratio',
        'source': 'inventory'
    }
}
```

### Feature Store Operations

#### Feature Ingestion
```python
async def ingest_customer_features():
    """Ingest customer features into feature store."""
    
    # Extract features from source data
    features_df = extract_customer_features(source_data)
    
    # Validate feature quality
    validation_results = validate_feature_quality(features_df)
    if not validation_results.passed:
        raise ValueError(f"Feature validation failed: {validation_results.errors}")
    
    # Ingest into feature store
    await feature_store.ingest_features(
        feature_group="customer_features",
        version="1.0",
        features=features_df,
        metadata={
            'source': 'transactions_warehouse',
            'extraction_date': datetime.now(),
            'validation_passed': True
        }
    )
```

#### Feature Retrieval
```python
async def get_customer_features(customer_id, as_of_date=None):
    """Retrieve customer features for inference."""
    
    entity_keys = {'customer_id': customer_id}
    
    features = await feature_store.get_features(
        feature_group="customer_features",
        version="1.0",
        entity_keys=entity_keys,
        as_of_date=as_of_date
    )
    
    return features
```

## Model Training & Validation

### Training Framework
**File**: `src/ml/training/model_trainer.py`

#### AutoML Training
```python
class AutoMLTrainer:
    """Automated machine learning trainer with hyperparameter optimization."""
    
    def __init__(self, model_type):
        self.model_type = model_type
        self.search_spaces = self.get_search_spaces()
        
    def train_models(self, X, y, cv_folds=5):
        """Train multiple models with hyperparameter tuning."""
        
        results = []
        
        for model_name, model_class in self.get_model_classes().items():
            print(f"Training {model_name}...")
            
            # Hyperparameter optimization
            search_space = self.search_spaces[model_name]
            best_params = self.optimize_hyperparameters(
                model_class, X, y, search_space, cv_folds
            )
            
            # Train final model
            model = model_class(**best_params)
            cv_scores = cross_val_score(model, X, y, cv=cv_folds)
            model.fit(X, y)
            
            results.append({
                'model_name': model_name,
                'model': model,
                'params': best_params,
                'cv_score_mean': cv_scores.mean(),
                'cv_score_std': cv_scores.std(),
                'validation_score': self.validate_model(model, X, y)
            })
        
        # Sort by performance
        results.sort(key=lambda x: x['cv_score_mean'], reverse=True)
        return results
```

#### Model Validation Framework
```python
class ModelValidator:
    """Comprehensive model validation framework."""
    
    def __init__(self, validation_config):
        self.config = validation_config
        self.validators = {
            'performance': self.validate_performance,
            'fairness': self.validate_fairness,
            'stability': self.validate_stability,
            'interpretability': self.validate_interpretability
        }
    
    def validate_performance(self, model, X_test, y_test):
        """Validate model performance metrics."""
        
        predictions = model.predict(X_test)
        probabilities = model.predict_proba(X_test) if hasattr(model, 'predict_proba') else None
        
        metrics = {
            'accuracy': accuracy_score(y_test, predictions),
            'precision': precision_score(y_test, predictions, average='weighted'),
            'recall': recall_score(y_test, predictions, average='weighted'),
            'f1_score': f1_score(y_test, predictions, average='weighted')
        }
        
        if probabilities is not None:
            metrics['roc_auc'] = roc_auc_score(y_test, probabilities[:, 1])
        
        # Check against thresholds
        passed = all(
            metrics[metric] >= self.config.performance_thresholds.get(metric, 0)
            for metric in metrics
        )
        
        return {
            'passed': passed,
            'metrics': metrics,
            'details': 'Performance validation results'
        }
    
    def validate_fairness(self, model, X_test, y_test, sensitive_features):
        """Validate model fairness across different groups."""
        
        predictions = model.predict(X_test)
        fairness_metrics = {}
        
        for feature in sensitive_features:
            groups = X_test[feature].unique()
            group_metrics = {}
            
            for group in groups:
                mask = X_test[feature] == group
                if mask.sum() > 0:
                    group_accuracy = accuracy_score(y_test[mask], predictions[mask])
                    group_metrics[str(group)] = group_accuracy
            
            fairness_metrics[feature] = group_metrics
        
        return {
            'passed': True,  # Implement specific fairness criteria
            'metrics': fairness_metrics,
            'details': 'Fairness validation results'
        }
```

## Model Deployment & Serving

### Deployment Architecture
```
Model Registry → Model Server → Load Balancer → API Gateway → Client Applications
      │              │              │             │
      └───── A/B Testing ────── Monitoring ──── Logging
```

### Model Server
**File**: `src/ml/deployment/model_server.py`

```python
class ModelServer:
    """Production model serving infrastructure."""
    
    def __init__(self, model_registry):
        self.model_registry = model_registry
        self.loaded_models = {}
        self.model_cache = {}
        
    async def load_model(self, model_id, version="latest"):
        """Load model from registry into memory."""
        
        if model_id in self.loaded_models:
            return self.loaded_models[model_id]
        
        # Load model from registry
        model_metadata = await self.model_registry.get_model(model_id, version)
        model = joblib.load(model_metadata.model_path)
        
        # Cache model
        self.loaded_models[model_id] = {
            'model': model,
            'metadata': model_metadata,
            'loaded_at': datetime.now(),
            'request_count': 0
        }
        
        return self.loaded_models[model_id]
    
    async def predict(self, model_id, input_data, request_id=None):
        """Make prediction using loaded model."""
        
        start_time = time.time()
        
        try:
            # Load model if not in memory
            model_info = await self.load_model(model_id)
            model = model_info['model']
            
            # Prepare input data
            if isinstance(input_data, dict):
                input_df = pd.DataFrame([input_data])
            else:
                input_df = pd.DataFrame(input_data)
            
            # Make prediction
            prediction = model.predict(input_df)
            
            # Get prediction probabilities if available
            probabilities = None
            if hasattr(model, 'predict_proba'):
                probabilities = model.predict_proba(input_df)
            
            # Update metrics
            model_info['request_count'] += 1
            inference_time = (time.time() - start_time) * 1000
            
            # Log prediction
            await self.log_prediction(
                model_id=model_id,
                request_id=request_id,
                input_data=input_data,
                prediction=prediction.tolist(),
                probabilities=probabilities.tolist() if probabilities is not None else None,
                inference_time_ms=inference_time
            )
            
            return {
                'prediction': prediction.tolist(),
                'probabilities': probabilities.tolist() if probabilities is not None else None,
                'model_id': model_id,
                'model_version': model_info['metadata'].version,
                'inference_time_ms': inference_time,
                'request_id': request_id
            }
            
        except Exception as e:
            # Log error
            await self.log_error(model_id, request_id, str(e))
            raise
```

### Deployment Strategies

#### 1. Blue-Green Deployment
```python
class BlueGreenDeployment:
    """Blue-green deployment strategy for ML models."""
    
    def __init__(self, model_server):
        self.model_server = model_server
        self.active_slot = "blue"
        self.slots = {"blue": {}, "green": {}}
    
    async def deploy_model(self, model_id, version, slot=None):
        """Deploy model to specified slot."""
        
        target_slot = slot or ("green" if self.active_slot == "blue" else "blue")
        
        # Deploy to target slot
        await self.model_server.load_model_to_slot(model_id, version, target_slot)
        
        # Run health checks
        health_ok = await self.run_health_checks(target_slot)
        
        if health_ok:
            # Switch traffic
            await self.switch_traffic(target_slot)
            self.active_slot = target_slot
        else:
            raise DeploymentError("Health checks failed")
```

#### 2. Canary Deployment
```python
class CanaryDeployment:
    """Canary deployment with gradual traffic shifting."""
    
    def __init__(self, model_server, traffic_router):
        self.model_server = model_server
        self.traffic_router = traffic_router
        
    async def deploy_canary(self, model_id, new_version, traffic_percent=10):
        """Deploy new model version as canary."""
        
        # Deploy new version
        await self.model_server.deploy_model(model_id, new_version, "canary")
        
        # Route traffic gradually
        await self.traffic_router.set_traffic_split({
            "stable": 100 - traffic_percent,
            "canary": traffic_percent
        })
        
        # Monitor canary performance
        await self.monitor_canary_performance(model_id, new_version)
```

## Model Monitoring & Observability

### Monitoring Architecture
```
Model Predictions → Data Collection → Drift Detection → Alert Generation
      │                    │               │               │
      └──── Performance ──── Quality ──── Metrics ──── Dashboard
```

### Model Monitor
**File**: `src/ml/monitoring/model_monitor.py`

```python
class ModelMonitor:
    """Comprehensive model monitoring system."""
    
    def __init__(self, model_id, config):
        self.model_id = model_id
        self.config = config
        self.drift_detector = DriftDetector(config.drift_config)
        self.performance_tracker = PerformanceTracker()
        self.alerting_system = AlertingSystem()
        
    async def monitor_batch(self, current_data):
        """Monitor a batch of data for drift and performance issues."""
        
        monitoring_results = []
        
        # Feature drift detection
        if self.config.enable_feature_drift:
            feature_drift = await self.drift_detector.detect_feature_drift(
                self.reference_data, current_data
            )
            monitoring_results.append(feature_drift)
        
        # Prediction drift detection
        if self.config.enable_prediction_drift:
            predictions = await self.model_server.predict_batch(current_data)
            prediction_drift = await self.drift_detector.detect_prediction_drift(
                self.reference_predictions, predictions
            )
            monitoring_results.append(prediction_drift)
        
        # Performance monitoring
        if self.config.enable_performance_monitoring:
            performance_metrics = await self.performance_tracker.calculate_metrics(
                current_data, predictions
            )
            monitoring_results.append(performance_metrics)
        
        # Check alert conditions
        await self.check_alert_conditions(monitoring_results)
        
        return monitoring_results
    
    async def detect_drift(self, reference_data, current_data):
        """Detect data drift using multiple statistical tests."""
        
        drift_results = []
        
        for feature in current_data.columns:
            if feature in reference_data.columns:
                # Kolmogorov-Smirnov test
                ks_statistic, ks_p_value = ks_2samp(
                    reference_data[feature].dropna(),
                    current_data[feature].dropna()
                )
                
                # PSI (Population Stability Index)
                psi_value = self.calculate_psi(
                    reference_data[feature],
                    current_data[feature]
                )
                
                # Wasserstein distance
                wasserstein_dist = wasserstein_distance(
                    reference_data[feature].dropna(),
                    current_data[feature].dropna()
                )
                
                drift_detected = (
                    ks_p_value < self.config.drift_threshold or
                    psi_value > self.config.psi_threshold or
                    wasserstein_dist > self.config.wasserstein_threshold
                )
                
                drift_results.append({
                    'feature': feature,
                    'drift_detected': drift_detected,
                    'ks_statistic': ks_statistic,
                    'ks_p_value': ks_p_value,
                    'psi_value': psi_value,
                    'wasserstein_distance': wasserstein_dist,
                    'drift_score': max(psi_value, 1 - ks_p_value, wasserstein_dist / 10)
                })
        
        return drift_results
```

### Performance Metrics
```python
class PerformanceTracker:
    """Track model performance metrics over time."""
    
    def __init__(self):
        self.metrics_history = []
        
    async def calculate_metrics(self, predictions, actuals=None):
        """Calculate comprehensive performance metrics."""
        
        metrics = {
            'timestamp': datetime.now(),
            'prediction_count': len(predictions),
            'prediction_mean': np.mean(predictions),
            'prediction_std': np.std(predictions),
            'prediction_percentiles': {
                'p25': np.percentile(predictions, 25),
                'p50': np.percentile(predictions, 50),
                'p75': np.percentile(predictions, 75),
                'p95': np.percentile(predictions, 95)
            }
        }
        
        # If actuals are available, calculate accuracy metrics
        if actuals is not None:
            metrics.update({
                'accuracy': accuracy_score(actuals, predictions),
                'precision': precision_score(actuals, predictions, average='weighted'),
                'recall': recall_score(actuals, predictions, average='weighted'),
                'f1_score': f1_score(actuals, predictions, average='weighted')
            })
        
        self.metrics_history.append(metrics)
        return metrics
    
    def detect_performance_degradation(self, window_size=100):
        """Detect performance degradation over time."""
        
        if len(self.metrics_history) < window_size * 2:
            return False
        
        # Compare recent performance with historical baseline
        recent_metrics = self.metrics_history[-window_size:]
        baseline_metrics = self.metrics_history[-(window_size*2):-window_size]
        
        # Calculate performance change
        recent_accuracy = np.mean([m['accuracy'] for m in recent_metrics if 'accuracy' in m])
        baseline_accuracy = np.mean([m['accuracy'] for m in baseline_metrics if 'accuracy' in m])
        
        if recent_accuracy and baseline_accuracy:
            performance_change = (recent_accuracy - baseline_accuracy) / baseline_accuracy
            return performance_change < -0.05  # 5% degradation threshold
        
        return False
```

## A/B Testing Framework

### A/B Testing Architecture
**File**: `src/ml/deployment/ab_testing.py`

```python
class ABTestingFramework:
    """A/B testing framework for ML models."""
    
    def __init__(self):
        self.active_experiments = {}
        self.traffic_router = TrafficRouter()
        self.metrics_collector = MetricsCollector()
        
    async def create_experiment(self, config: ExperimentConfig):
        """Create a new A/B test experiment."""
        
        experiment = {
            'id': config.experiment_id,
            'name': config.name,
            'start_date': config.start_date,
            'end_date': config.end_date,
            'control_model': config.control_model_id,
            'treatment_models': config.treatment_model_ids,
            'traffic_allocation': config.traffic_allocation,
            'primary_metric': config.primary_metric,
            'status': 'active',
            'results': {}
        }
        
        # Configure traffic routing
        await self.traffic_router.setup_experiment_routing(experiment)
        
        # Start metrics collection
        await self.metrics_collector.start_experiment_tracking(experiment['id'])
        
        self.active_experiments[config.experiment_id] = experiment
        
        return experiment
    
    async def get_experiment_results(self, experiment_id):
        """Get current experiment results and statistical significance."""
        
        experiment = self.active_experiments[experiment_id]
        
        # Collect metrics for all variants
        control_metrics = await self.metrics_collector.get_variant_metrics(
            experiment_id, 'control'
        )
        
        treatment_metrics = {}
        for i, treatment_model in enumerate(experiment['treatment_models']):
            treatment_metrics[f'treatment_{i}'] = await self.metrics_collector.get_variant_metrics(
                experiment_id, f'treatment_{i}'
            )
        
        # Calculate statistical significance
        significance_results = self.calculate_statistical_significance(
            control_metrics, treatment_metrics, experiment['primary_metric']
        )
        
        return {
            'experiment_id': experiment_id,
            'status': experiment['status'],
            'control_metrics': control_metrics,
            'treatment_metrics': treatment_metrics,
            'significance_results': significance_results,
            'recommendation': self.generate_recommendation(significance_results)
        }
    
    def calculate_statistical_significance(self, control_metrics, treatment_metrics, primary_metric):
        """Calculate statistical significance using t-test."""
        
        from scipy import stats
        
        results = {}
        control_values = control_metrics[primary_metric]['values']
        
        for variant_name, metrics in treatment_metrics.items():
            treatment_values = metrics[primary_metric]['values']
            
            # Perform t-test
            t_stat, p_value = stats.ttest_ind(control_values, treatment_values)
            
            # Calculate effect size (Cohen's d)
            pooled_std = np.sqrt(((len(control_values) - 1) * np.var(control_values) + 
                                 (len(treatment_values) - 1) * np.var(treatment_values)) / 
                                (len(control_values) + len(treatment_values) - 2))
            
            cohens_d = (np.mean(treatment_values) - np.mean(control_values)) / pooled_std
            
            # Calculate confidence interval
            confidence_interval = stats.t.interval(
                0.95, 
                len(control_values) + len(treatment_values) - 2,
                loc=np.mean(treatment_values) - np.mean(control_values),
                scale=pooled_std * np.sqrt(1/len(control_values) + 1/len(treatment_values))
            )
            
            results[variant_name] = {
                'p_value': p_value,
                'significant': p_value < 0.05,
                'effect_size': cohens_d,
                'confidence_interval': confidence_interval,
                'improvement': (np.mean(treatment_values) - np.mean(control_values)) / np.mean(control_values) * 100
            }
        
        return results
```

## Data Drift Detection

### Drift Detection Methods

#### 1. Statistical Tests
```python
class StatisticalDriftDetector:
    """Statistical methods for drift detection."""
    
    def __init__(self, config):
        self.config = config
        
    def kolmogorov_smirnov_test(self, reference_data, current_data):
        """KS test for distribution comparison."""
        
        from scipy.stats import ks_2samp
        
        ks_statistic, p_value = ks_2samp(reference_data, current_data)
        
        return {
            'method': 'kolmogorov_smirnov',
            'statistic': ks_statistic,
            'p_value': p_value,
            'drift_detected': p_value < self.config.significance_level,
            'drift_score': 1 - p_value
        }
    
    def population_stability_index(self, reference_data, current_data, bins=10):
        """Calculate Population Stability Index (PSI)."""
        
        # Create bins based on reference data
        bin_edges = np.histogram_bin_edges(reference_data, bins=bins)
        
        # Calculate distributions
        ref_counts, _ = np.histogram(reference_data, bins=bin_edges)
        cur_counts, _ = np.histogram(current_data, bins=bin_edges)
        
        # Convert to proportions
        ref_props = ref_counts / len(reference_data)
        cur_props = cur_counts / len(current_data)
        
        # Handle zero proportions
        ref_props = np.where(ref_props == 0, 0.0001, ref_props)
        cur_props = np.where(cur_props == 0, 0.0001, cur_props)
        
        # Calculate PSI
        psi = np.sum((cur_props - ref_props) * np.log(cur_props / ref_props))
        
        # Interpret PSI
        if psi < 0.1:
            drift_level = "low"
        elif psi < 0.2:
            drift_level = "medium"
        else:
            drift_level = "high"
        
        return {
            'method': 'population_stability_index',
            'psi_value': psi,
            'drift_level': drift_level,
            'drift_detected': psi > self.config.psi_threshold,
            'drift_score': min(psi / 0.2, 1.0)  # Normalize to 0-1
        }
```

#### 2. Distance-Based Methods
```python
def wasserstein_distance_drift(self, reference_data, current_data):
    """Wasserstein distance for drift detection."""
    
    from scipy.stats import wasserstein_distance
    
    distance = wasserstein_distance(reference_data, current_data)
    
    # Normalize distance by reference data range
    ref_range = np.max(reference_data) - np.min(reference_data)
    normalized_distance = distance / ref_range if ref_range > 0 else 0
    
    return {
        'method': 'wasserstein_distance',
        'distance': distance,
        'normalized_distance': normalized_distance,
        'drift_detected': normalized_distance > self.config.wasserstein_threshold,
        'drift_score': min(normalized_distance / 0.1, 1.0)
    }
```

### Advanced Drift Detection
```python
class AdvancedDriftDetector:
    """Advanced drift detection using machine learning."""
    
    def __init__(self):
        self.classifier = None
        self.feature_importance = None
        
    def train_drift_detector(self, reference_data, current_data):
        """Train classifier to detect drift."""
        
        # Create binary classification dataset
        X_ref = reference_data.copy()
        X_cur = current_data.copy()
        
        X_ref['label'] = 0  # Reference
        X_cur['label'] = 1  # Current
        
        X_combined = pd.concat([X_ref, X_cur], ignore_index=True)
        
        # Features and target
        X = X_combined.drop('label', axis=1)
        y = X_combined['label']
        
        # Train classifier
        from sklearn.ensemble import RandomForestClassifier
        self.classifier = RandomForestClassifier(n_estimators=100, random_state=42)
        self.classifier.fit(X, y)
        
        # Get feature importance
        self.feature_importance = dict(zip(X.columns, self.classifier.feature_importances_))
        
        # Calculate drift score (AUC)
        from sklearn.metrics import roc_auc_score
        y_pred_proba = self.classifier.predict_proba(X)[:, 1]
        auc_score = roc_auc_score(y, y_pred_proba)
        
        return {
            'method': 'classifier_based',
            'auc_score': auc_score,
            'drift_detected': auc_score > 0.75,  # Significant drift
            'drift_score': (auc_score - 0.5) * 2,  # Normalize to 0-1
            'feature_importance': self.feature_importance
        }
```

## MLOps Automation

### Continuous Integration/Continuous Deployment

#### 1. Model Training Pipeline
```yaml
# .github/workflows/model-training.yml
name: Model Training Pipeline

on:
  schedule:
    - cron: '0 2 * * 0'  # Weekly on Sunday at 2 AM
  workflow_dispatch:

jobs:
  train-models:
    runs-on: ubuntu-latest
    
    steps:
    - name: Checkout code
      uses: actions/checkout@v3
      
    - name: Setup Python
      uses: actions/setup-python@v4
      with:
        python-version: '3.9'
        
    - name: Install dependencies
      run: |
        pip install -r requirements.txt
        
    - name: Run data validation
      run: |
        python scripts/validate_data.py
        
    - name: Train models
      run: |
        python scripts/train_models.py
        
    - name: Validate models
      run: |
        python scripts/validate_models.py
        
    - name: Register models
      run: |
        python scripts/register_models.py
      env:
        MODEL_REGISTRY_URL: ${{ secrets.MODEL_REGISTRY_URL }}
```

#### 2. Model Deployment Pipeline
```python
# scripts/deploy_model.py
import asyncio
from src.ml.deployment.model_server import ModelServer
from src.ml.deployment.ab_testing import ABTestingFramework

async def deploy_model():
    """Automated model deployment with A/B testing."""
    
    # Load configuration
    config = load_deployment_config()
    
    # Initialize components
    model_server = ModelServer()
    ab_testing = ABTestingFramework()
    
    # Deploy new model version
    new_model_id = config['new_model_id']
    current_model_id = config['current_model_id']
    
    # Start A/B test
    experiment_config = {
        'experiment_id': f'model_comparison_{datetime.now().strftime("%Y%m%d")}',
        'name': f'Deploy {new_model_id}',
        'control_model_id': current_model_id,
        'treatment_model_ids': [new_model_id],
        'traffic_allocation': {'control': 0.9, 'treatment_0': 0.1},
        'duration_days': 7
    }
    
    experiment = await ab_testing.create_experiment(experiment_config)
    
    print(f"Started A/B test experiment: {experiment['id']}")
    
    # Monitor experiment for 24 hours
    await monitor_experiment(experiment['id'], duration_hours=24)

if __name__ == "__main__":
    asyncio.run(deploy_model())
```

### Automated Retraining
```python
class AutoRetrainingSystem:
    """Automated model retraining system."""
    
    def __init__(self, config):
        self.config = config
        self.drift_monitor = DriftMonitor()
        self.performance_monitor = PerformanceMonitor()
        self.trainer = ModelTrainer()
        
    async def check_retrain_conditions(self, model_id):
        """Check if model needs retraining."""
        
        retrain_needed = False
        reasons = []
        
        # Check performance degradation
        performance_metrics = await self.performance_monitor.get_recent_metrics(model_id)
        if performance_metrics['accuracy'] < self.config.performance_threshold:
            retrain_needed = True
            reasons.append('performance_degradation')
        
        # Check data drift
        drift_results = await self.drift_monitor.check_drift(model_id)
        if drift_results['significant_drift']:
            retrain_needed = True
            reasons.append('data_drift')
        
        # Check time-based retraining
        last_training_date = await self.get_last_training_date(model_id)
        days_since_training = (datetime.now() - last_training_date).days
        if days_since_training > self.config.max_days_without_retraining:
            retrain_needed = True
            reasons.append('time_based')
        
        return {
            'retrain_needed': retrain_needed,
            'reasons': reasons,
            'confidence': self.calculate_retrain_confidence(reasons)
        }
    
    async def trigger_retraining(self, model_id, reasons):
        """Trigger automated model retraining."""
        
        logger.info(f"Starting automated retraining for model {model_id}")
        logger.info(f"Reasons: {reasons}")
        
        # Get fresh training data
        training_data = await self.get_latest_training_data()
        
        # Train new model version
        new_model = await self.trainer.train_model(
            model_type=self.config.model_type,
            training_data=training_data,
            hyperparameters=self.config.hyperparameters
        )
        
        # Validate new model
        validation_results = await self.trainer.validate_model(new_model)
        
        if validation_results['passed']:
            # Register new model version
            new_model_id = await self.register_model(new_model)
            
            # Start gradual rollout
            await self.start_gradual_rollout(model_id, new_model_id)
            
            logger.info(f"Successfully retrained and deployed model {new_model_id}")
            
        else:
            logger.error(f"Model validation failed: {validation_results['errors']}")
            # Send alert to ML team
            await self.send_alert('retrain_validation_failed', validation_results)
```

## Security & Governance

### ML Security Framework
```python
class MLSecurityFramework:
    """Security framework for ML operations."""
    
    def __init__(self):
        self.access_control = MLAccessControl()
        self.audit_logger = MLAuditLogger()
        self.data_protection = MLDataProtection()
        
    async def secure_model_access(self, user_id, model_id, operation):
        """Secure access control for ML models."""
        
        # Check user permissions
        has_permission = await self.access_control.check_permission(
            user_id, f"model:{model_id}:{operation}"
        )
        
        if not has_permission:
            await self.audit_logger.log_access_denied(user_id, model_id, operation)
            raise PermissionError(f"Access denied for operation {operation} on model {model_id}")
        
        # Log access
        await self.audit_logger.log_model_access(user_id, model_id, operation)
        
        return True
    
    async def protect_training_data(self, data):
        """Apply data protection measures during training."""
        
        # Detect sensitive information
        sensitive_columns = await self.data_protection.detect_sensitive_data(data)
        
        if sensitive_columns:
            # Apply appropriate protection
            protected_data = await self.data_protection.apply_protection(
                data, sensitive_columns
            )
            
            # Log data protection actions
            await self.audit_logger.log_data_protection(sensitive_columns)
            
            return protected_data
        
        return data
```

### Model Governance
```python
class ModelGovernance:
    """Model governance and compliance framework."""
    
    def __init__(self):
        self.model_registry = ModelRegistry()
        self.compliance_checker = ComplianceChecker()
        self.risk_assessor = RiskAssessor()
        
    async def register_model(self, model, metadata):
        """Register model with governance checks."""
        
        # Validate model metadata
        validation_results = self.validate_model_metadata(metadata)
        if not validation_results['valid']:
            raise ValueError(f"Invalid metadata: {validation_results['errors']}")
        
        # Assess model risks
        risk_assessment = await self.risk_assessor.assess_model(model, metadata)
        
        # Check compliance requirements
        compliance_results = await self.compliance_checker.check_model(model, metadata)
        
        # Add governance metadata
        governance_metadata = {
            'risk_level': risk_assessment['risk_level'],
            'compliance_status': compliance_results['status'],
            'approval_required': risk_assessment['risk_level'] in ['high', 'critical'],
            'registered_by': metadata.get('user_id'),
            'registration_date': datetime.now(),
            'governance_version': '1.0'
        }
        
        # Register model
        model_id = await self.model_registry.register_model(
            model, {**metadata, **governance_metadata}
        )
        
        # Send for approval if required
        if governance_metadata['approval_required']:
            await self.submit_for_approval(model_id, risk_assessment)
        
        return model_id
```

## Performance Optimization

### Model Performance Optimization
```python
class ModelOptimizer:
    """Model performance optimization techniques."""
    
    def __init__(self):
        self.optimization_techniques = {
            'quantization': self.apply_quantization,
            'pruning': self.apply_pruning,
            'distillation': self.apply_distillation,
            'onnx_conversion': self.convert_to_onnx
        }
    
    def apply_quantization(self, model):
        """Apply model quantization for faster inference."""
        
        # Dynamic quantization for PyTorch models
        if hasattr(model, 'state_dict'):
            import torch.quantization as quantization
            quantized_model = quantization.quantize_dynamic(
                model, {torch.nn.Linear}, dtype=torch.qint8
            )
            return quantized_model
        
        # TensorFlow Lite quantization
        elif hasattr(model, 'save'):
            import tensorflow as tf
            converter = tf.lite.TFLiteConverter.from_saved_model(model)
            converter.optimizations = [tf.lite.Optimize.DEFAULT]
            quantized_tflite_model = converter.convert()
            return quantized_tflite_model
        
        return model
    
    def benchmark_model(self, model, test_data, num_iterations=100):
        """Benchmark model inference performance."""
        
        import time
        
        # Warm up
        for _ in range(10):
            _ = model.predict(test_data[:1])
        
        # Measure inference time
        start_time = time.time()
        for _ in range(num_iterations):
            predictions = model.predict(test_data)
        end_time = time.time()
        
        avg_inference_time = (end_time - start_time) / num_iterations
        throughput = len(test_data) / avg_inference_time
        
        return {
            'avg_inference_time_ms': avg_inference_time * 1000,
            'throughput_samples_per_second': throughput,
            'model_size_mb': self.get_model_size(model),
            'test_samples': len(test_data)
        }
```

### Scaling Strategies
```python
class MLScalingStrategy:
    """Scaling strategies for ML workloads."""
    
    def __init__(self):
        self.load_balancer = LoadBalancer()
        self.auto_scaler = AutoScaler()
        self.cache_manager = CacheManager()
        
    async def scale_model_serving(self, model_id, traffic_metrics):
        """Auto-scale model serving infrastructure."""
        
        current_load = traffic_metrics['requests_per_second']
        current_replicas = await self.get_current_replicas(model_id)
        
        # Calculate required replicas
        target_rps_per_replica = 100  # Configure based on model capacity
        required_replicas = max(1, int(current_load / target_rps_per_replica) + 1)
        
        if required_replicas != current_replicas:
            await self.auto_scaler.scale_replicas(model_id, required_replicas)
            
            logger.info(f"Scaled model {model_id} from {current_replicas} to {required_replicas} replicas")
        
        # Update load balancer configuration
        await self.load_balancer.update_configuration(model_id, required_replicas)
    
    async def optimize_batch_processing(self, batch_size, model_complexity):
        """Optimize batch processing parameters."""
        
        # Dynamic batch sizing based on model complexity
        if model_complexity == 'low':
            optimal_batch_size = min(batch_size, 1000)
        elif model_complexity == 'medium':
            optimal_batch_size = min(batch_size, 500)
        else:  # high complexity
            optimal_batch_size = min(batch_size, 100)
        
        return {
            'batch_size': optimal_batch_size,
            'parallel_workers': self.calculate_optimal_workers(optimal_batch_size),
            'memory_limit': self.estimate_memory_requirements(optimal_batch_size, model_complexity)
        }
```

## Troubleshooting

### Common Issues and Solutions

#### 1. Model Performance Degradation
**Symptoms**: Decreasing accuracy, increasing error rates
**Diagnosis**:
```python
async def diagnose_performance_issues(model_id):
    """Diagnose model performance issues."""
    
    diagnostics = {}
    
    # Check data drift
    drift_results = await drift_detector.check_drift(model_id)
    diagnostics['data_drift'] = drift_results
    
    # Check feature distribution changes
    feature_stats = await get_feature_statistics(model_id)
    diagnostics['feature_changes'] = feature_stats
    
    # Check prediction distribution
    prediction_stats = await get_prediction_statistics(model_id)
    diagnostics['prediction_drift'] = prediction_stats
    
    # Check model staleness
    last_training_date = await get_last_training_date(model_id)
    days_old = (datetime.now() - last_training_date).days
    diagnostics['model_age_days'] = days_old
    
    return diagnostics
```

**Solutions**:
- Retrain model with recent data
- Update feature engineering pipeline
- Adjust model hyperparameters
- Implement continuous learning

#### 2. High Inference Latency
**Symptoms**: Slow model predictions, timeouts
**Diagnosis**:
```python
def diagnose_latency_issues(model_id):
    """Diagnose model inference latency issues."""
    
    # Profile model inference
    profiling_results = profile_model_inference(model_id)
    
    # Check resource utilization
    resource_usage = get_resource_utilization(model_id)
    
    # Analyze bottlenecks
    bottlenecks = identify_bottlenecks(profiling_results)
    
    return {
        'profiling': profiling_results,
        'resources': resource_usage,
        'bottlenecks': bottlenecks,
        'recommendations': generate_optimization_recommendations(bottlenecks)
    }
```

**Solutions**:
- Model quantization or pruning
- Batch inference optimization
- Caching frequently used features
- Hardware acceleration (GPU/TPU)

#### 3. Memory Issues
**Symptoms**: Out-of-memory errors, high memory usage
**Solutions**:
```python
def optimize_memory_usage():
    """Memory optimization strategies."""
    
    strategies = [
        'Use memory-efficient data loading (streaming)',
        'Implement gradient checkpointing',
        'Use mixed precision training',
        'Optimize batch sizes',
        'Clear unused variables',
        'Use model parallelism for large models'
    ]
    
    return strategies
```

### Monitoring and Alerting Setup
```python
# monitoring/ml_alerts.py
class MLAlertingSystem:
    """Comprehensive alerting for ML systems."""
    
    def __init__(self):
        self.alert_rules = self.setup_alert_rules()
        
    def setup_alert_rules(self):
        """Define ML-specific alert rules."""
        
        return {
            'model_performance': {
                'metric': 'accuracy',
                'threshold': 0.85,
                'operator': 'less_than',
                'severity': 'high'
            },
            'data_drift': {
                'metric': 'drift_score',
                'threshold': 0.1,
                'operator': 'greater_than',
                'severity': 'medium'
            },
            'inference_latency': {
                'metric': 'p95_latency_ms',
                'threshold': 1000,
                'operator': 'greater_than',
                'severity': 'high'
            },
            'error_rate': {
                'metric': 'error_rate',
                'threshold': 0.05,
                'operator': 'greater_than',
                'severity': 'critical'
            }
        }
```

### Best Practices

#### 1. Model Development
- Use version control for all code, data, and models
- Implement comprehensive testing for ML code
- Document model assumptions and limitations
- Use feature flags for gradual rollouts

#### 2. Production Deployment
- Implement proper monitoring and alerting
- Use A/B testing for model comparisons
- Maintain model lineage and governance
- Plan for model rollback scenarios

#### 3. Data Management
- Validate data quality continuously
- Monitor for data drift and distribution changes
- Implement proper data versioning
- Ensure data privacy and compliance

#### 4. Performance Optimization
- Profile models regularly
- Optimize for target hardware
- Use appropriate batch sizes
- Implement caching strategies

This comprehensive MLOps guide provides the foundation for managing machine learning operations at enterprise scale, ensuring reliable, scalable, and governed ML systems in production.