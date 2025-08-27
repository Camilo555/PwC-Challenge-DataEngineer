"""
Real-time ML Model Serving Infrastructure

High-performance model serving system with load balancing, caching,
and automatic scaling capabilities.
"""

import logging
import asyncio
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Union, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from pathlib import Path
import json
import joblib
import pickle
from concurrent.futures import ThreadPoolExecutor
import uvicorn
from fastapi import FastAPI, HTTPException, BackgroundTasks, Depends
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
import aioredis
from sqlalchemy.ext.asyncio import AsyncSession

from src.core.config import get_settings
from src.core.logging import get_logger
from src.core.monitoring.metrics import MetricsCollector
from src.core.caching.redis_cache import get_redis_client
from src.ml.feature_engineering.feature_pipeline import FeatureEngineeringPipeline
from src.ml.feature_engineering.feature_store import FeatureStore, FeatureStoreClient

logger = get_logger(__name__)
settings = get_settings()


@dataclass
class ModelMetadata:
    """Model metadata for serving."""
    
    model_id: str
    model_name: str
    model_type: str
    version: str
    problem_type: str
    input_schema: Dict[str, Any]
    output_schema: Dict[str, Any]
    feature_pipeline_path: Optional[str] = None
    model_path: str = ""
    created_at: datetime = field(default_factory=datetime.utcnow)
    last_used: datetime = field(default_factory=datetime.utcnow)
    usage_count: int = 0
    avg_latency_ms: float = 0.0
    
    
class PredictionRequest(BaseModel):
    """Request model for predictions."""
    
    model_id: str
    features: Dict[str, Any]
    return_probabilities: bool = False
    return_feature_importance: bool = False
    

class BatchPredictionRequest(BaseModel):
    """Request model for batch predictions."""
    
    model_id: str
    features: List[Dict[str, Any]]
    return_probabilities: bool = False
    return_feature_importance: bool = False


class PredictionResponse(BaseModel):
    """Response model for predictions."""
    
    prediction: Union[float, int, str, List[Union[float, int, str]]]
    probabilities: Optional[Dict[str, float]] = None
    feature_importance: Optional[Dict[str, float]] = None
    model_id: str
    model_version: str
    timestamp: datetime
    latency_ms: float


class ModelRegistry:
    """Registry for managing deployed models."""
    
    def __init__(self):
        self.models: Dict[str, ModelMetadata] = {}
        self.loaded_models: Dict[str, Any] = {}
        self.feature_pipelines: Dict[str, FeatureEngineeringPipeline] = {}
        self.redis_client = get_redis_client()
        self.metrics_collector = MetricsCollector()
        
    async def register_model(self, metadata: ModelMetadata) -> bool:
        """Register a model in the registry."""
        try:
            self.models[metadata.model_id] = metadata
            
            # Store in Redis for persistence
            model_data = {
                "model_id": metadata.model_id,
                "model_name": metadata.model_name,
                "model_type": metadata.model_type,
                "version": metadata.version,
                "problem_type": metadata.problem_type,
                "input_schema": metadata.input_schema,
                "output_schema": metadata.output_schema,
                "model_path": metadata.model_path,
                "feature_pipeline_path": metadata.feature_pipeline_path,
                "created_at": metadata.created_at.isoformat()
            }
            
            await self.redis_client.hset(
                f"model_registry:{metadata.model_id}",
                mapping={k: json.dumps(v, default=str) for k, v in model_data.items()}
            )
            
            logger.info(f"Model registered: {metadata.model_name} v{metadata.version}")
            
            self.metrics_collector.increment_counter(
                "model_registry_models_registered_total",
                tags={"model_name": metadata.model_name, "model_type": metadata.model_type}
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Error registering model: {str(e)}")
            return False
    
    async def load_model(self, model_id: str) -> bool:
        """Load model into memory for serving."""
        try:
            if model_id not in self.models:
                # Try to load from Redis
                model_data = await self.redis_client.hgetall(f"model_registry:{model_id}")
                if not model_data:
                    return False
                
                # Reconstruct metadata
                metadata = ModelMetadata(
                    model_id=json.loads(model_data[b'model_id'].decode()),
                    model_name=json.loads(model_data[b'model_name'].decode()),
                    model_type=json.loads(model_data[b'model_type'].decode()),
                    version=json.loads(model_data[b'version'].decode()),
                    problem_type=json.loads(model_data[b'problem_type'].decode()),
                    input_schema=json.loads(model_data[b'input_schema'].decode()),
                    output_schema=json.loads(model_data[b'output_schema'].decode()),
                    model_path=json.loads(model_data[b'model_path'].decode()),
                    feature_pipeline_path=json.loads(model_data[b'feature_pipeline_path'].decode()) if b'feature_pipeline_path' in model_data else None,
                    created_at=datetime.fromisoformat(json.loads(model_data[b'created_at'].decode()))
                )
                self.models[model_id] = metadata
            
            metadata = self.models[model_id]
            
            # Load the actual model
            if model_id not in self.loaded_models:
                model_path = Path(metadata.model_path)
                if not model_path.exists():
                    logger.error(f"Model file not found: {metadata.model_path}")
                    return False
                
                # Load model
                with open(model_path, 'rb') as f:
                    model = joblib.load(f)
                
                self.loaded_models[model_id] = model
                
                # Load feature pipeline if exists
                if metadata.feature_pipeline_path:
                    pipeline_path = Path(metadata.feature_pipeline_path)
                    if pipeline_path.exists():
                        with open(pipeline_path, 'rb') as f:
                            pipeline = joblib.load(f)
                        self.feature_pipelines[model_id] = pipeline
                
                logger.info(f"Model loaded into memory: {metadata.model_name}")
                
                self.metrics_collector.increment_counter(
                    "model_registry_models_loaded_total",
                    tags={"model_name": metadata.model_name, "model_type": metadata.model_type}
                )
            
            return True
            
        except Exception as e:
            logger.error(f"Error loading model {model_id}: {str(e)}")
            return False
    
    async def unload_model(self, model_id: str) -> bool:
        """Unload model from memory."""
        try:
            if model_id in self.loaded_models:
                del self.loaded_models[model_id]
            
            if model_id in self.feature_pipelines:
                del self.feature_pipelines[model_id]
            
            logger.info(f"Model unloaded from memory: {model_id}")
            
            self.metrics_collector.increment_counter(
                "model_registry_models_unloaded_total",
                tags={"model_id": model_id}
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Error unloading model {model_id}: {str(e)}")
            return False
    
    def get_model(self, model_id: str) -> Optional[Any]:
        """Get loaded model."""
        return self.loaded_models.get(model_id)
    
    def get_feature_pipeline(self, model_id: str) -> Optional[FeatureEngineeringPipeline]:
        """Get feature pipeline for model."""
        return self.feature_pipelines.get(model_id)
    
    def get_metadata(self, model_id: str) -> Optional[ModelMetadata]:
        """Get model metadata."""
        return self.models.get(model_id)
    
    async def update_usage_stats(self, model_id: str, latency_ms: float):
        """Update model usage statistics."""
        if model_id in self.models:
            metadata = self.models[model_id]
            metadata.last_used = datetime.utcnow()
            metadata.usage_count += 1
            
            # Update rolling average latency
            alpha = 0.1  # Smoothing factor
            metadata.avg_latency_ms = (
                alpha * latency_ms + (1 - alpha) * metadata.avg_latency_ms
            )


class ModelServer:
    """High-performance model serving server."""
    
    def __init__(self, registry: ModelRegistry, feature_store: Optional[FeatureStore] = None):
        self.registry = registry
        self.feature_store = feature_store
        self.feature_store_client = FeatureStoreClient(feature_store) if feature_store else None
        self.executor = ThreadPoolExecutor(max_workers=4)
        self.metrics_collector = MetricsCollector()
        
    async def predict(self, request: PredictionRequest) -> PredictionResponse:
        """Make single prediction."""
        start_time = datetime.utcnow()
        
        try:
            # Ensure model is loaded
            if not await self.registry.load_model(request.model_id):
                raise HTTPException(status_code=404, detail="Model not found or failed to load")
            
            model = self.registry.get_model(request.model_id)
            feature_pipeline = self.registry.get_feature_pipeline(request.model_id)
            metadata = self.registry.get_metadata(request.model_id)
            
            if not model or not metadata:
                raise HTTPException(status_code=404, detail="Model not found")
            
            # Prepare features
            features_df = pd.DataFrame([request.features])
            
            # Get additional features from feature store if available
            if self.feature_store_client:
                # Extract entity keys from features
                entity_keys = {key: request.features.get(key) for key in request.features.keys()}
                
                # Get additional features (this is a simplified example)
                try:
                    additional_features = await self.feature_store_client.get_online_features(
                        [{"name": "customer_features", "version": "1.0"}],
                        entity_keys
                    )
                    
                    if additional_features:
                        features_df = features_df.assign(**additional_features)
                        
                except Exception as e:
                    logger.warning(f"Could not fetch additional features: {str(e)}")
            
            # Apply feature pipeline
            if feature_pipeline:
                features_df = feature_pipeline.transform(features_df)
            
            # Make prediction in thread pool
            loop = asyncio.get_event_loop()
            
            if metadata.problem_type == "classification":
                if request.return_probabilities and hasattr(model, 'predict_proba'):
                    prediction, probabilities = await loop.run_in_executor(
                        self.executor, self._predict_with_proba, model, features_df
                    )
                else:
                    prediction = await loop.run_in_executor(
                        self.executor, model.predict, features_df
                    )
                    probabilities = None
            else:  # regression
                prediction = await loop.run_in_executor(
                    self.executor, model.predict, features_df
                )
                probabilities = None
            
            # Extract single prediction
            if isinstance(prediction, np.ndarray):
                prediction = prediction[0]
            
            # Feature importance
            feature_importance = None
            if request.return_feature_importance:
                if hasattr(model, 'feature_importances_'):
                    feature_importance = dict(zip(
                        features_df.columns, 
                        model.feature_importances_
                    ))
                elif hasattr(model, 'coef_'):
                    feature_importance = dict(zip(
                        features_df.columns,
                        np.abs(model.coef_.flatten())
                    ))
            
            # Calculate latency
            latency = (datetime.utcnow() - start_time).total_seconds() * 1000
            
            # Update usage stats
            await self.registry.update_usage_stats(request.model_id, latency)
            
            # Log metrics
            self.metrics_collector.increment_counter(
                "model_server_predictions_total",
                tags={"model_id": request.model_id, "model_type": metadata.model_type}
            )
            self.metrics_collector.record_histogram(
                "model_server_prediction_latency_ms",
                latency,
                tags={"model_id": request.model_id}
            )
            
            return PredictionResponse(
                prediction=prediction,
                probabilities=probabilities,
                feature_importance=feature_importance,
                model_id=request.model_id,
                model_version=metadata.version,
                timestamp=datetime.utcnow(),
                latency_ms=latency
            )
            
        except Exception as e:
            logger.error(f"Error making prediction: {str(e)}")
            self.metrics_collector.increment_counter(
                "model_server_prediction_errors_total",
                tags={"model_id": request.model_id}
            )
            raise HTTPException(status_code=500, detail=str(e))
    
    async def predict_batch(self, request: BatchPredictionRequest) -> List[PredictionResponse]:
        """Make batch predictions."""
        start_time = datetime.utcnow()
        
        try:
            # Ensure model is loaded
            if not await self.registry.load_model(request.model_id):
                raise HTTPException(status_code=404, detail="Model not found or failed to load")
            
            model = self.registry.get_model(request.model_id)
            feature_pipeline = self.registry.get_feature_pipeline(request.model_id)
            metadata = self.registry.get_metadata(request.model_id)
            
            if not model or not metadata:
                raise HTTPException(status_code=404, detail="Model not found")
            
            # Prepare features
            features_df = pd.DataFrame(request.features)
            
            # Apply feature pipeline
            if feature_pipeline:
                features_df = feature_pipeline.transform(features_df)
            
            # Make predictions in thread pool
            loop = asyncio.get_event_loop()
            
            if metadata.problem_type == "classification":
                if request.return_probabilities and hasattr(model, 'predict_proba'):
                    predictions, probabilities_array = await loop.run_in_executor(
                        self.executor, self._predict_batch_with_proba, model, features_df
                    )
                else:
                    predictions = await loop.run_in_executor(
                        self.executor, model.predict, features_df
                    )
                    probabilities_array = None
            else:  # regression
                predictions = await loop.run_in_executor(
                    self.executor, model.predict, features_df
                )
                probabilities_array = None
            
            # Feature importance
            feature_importance = None
            if request.return_feature_importance:
                if hasattr(model, 'feature_importances_'):
                    feature_importance = dict(zip(
                        features_df.columns, 
                        model.feature_importances_
                    ))
                elif hasattr(model, 'coef_'):
                    feature_importance = dict(zip(
                        features_df.columns,
                        np.abs(model.coef_.flatten())
                    ))
            
            # Calculate latency
            total_latency = (datetime.utcnow() - start_time).total_seconds() * 1000
            avg_latency = total_latency / len(predictions)
            
            # Create responses
            responses = []
            for i, prediction in enumerate(predictions):
                probabilities = None
                if probabilities_array is not None:
                    if len(probabilities_array.shape) > 1:
                        prob_dict = {}
                        for j, prob in enumerate(probabilities_array[i]):
                            prob_dict[f"class_{j}"] = float(prob)
                        probabilities = prob_dict
                
                responses.append(PredictionResponse(
                    prediction=prediction,
                    probabilities=probabilities,
                    feature_importance=feature_importance,
                    model_id=request.model_id,
                    model_version=metadata.version,
                    timestamp=datetime.utcnow(),
                    latency_ms=avg_latency
                ))
            
            # Update usage stats
            await self.registry.update_usage_stats(request.model_id, avg_latency)
            
            # Log metrics
            self.metrics_collector.increment_counter(
                "model_server_batch_predictions_total",
                tags={"model_id": request.model_id, "batch_size": str(len(predictions))}
            )
            self.metrics_collector.record_histogram(
                "model_server_batch_prediction_latency_ms",
                total_latency,
                tags={"model_id": request.model_id, "batch_size": str(len(predictions))}
            )
            
            return responses
            
        except Exception as e:
            logger.error(f"Error making batch predictions: {str(e)}")
            self.metrics_collector.increment_counter(
                "model_server_batch_prediction_errors_total",
                tags={"model_id": request.model_id}
            )
            raise HTTPException(status_code=500, detail=str(e))
    
    def _predict_with_proba(self, model, features_df):
        """Make prediction with probabilities."""
        prediction = model.predict(features_df)
        probabilities = model.predict_proba(features_df)
        
        # Convert probabilities to dict
        prob_dict = {}
        if len(probabilities.shape) > 1:
            classes = getattr(model, 'classes_', range(probabilities.shape[1]))
            for i, class_label in enumerate(classes):
                prob_dict[str(class_label)] = float(probabilities[0][i])
        
        return prediction[0] if len(prediction) > 0 else prediction, prob_dict
    
    def _predict_batch_with_proba(self, model, features_df):
        """Make batch predictions with probabilities."""
        predictions = model.predict(features_df)
        probabilities = model.predict_proba(features_df)
        return predictions, probabilities


# FastAPI app for model serving
app = FastAPI(title="ML Model Server", version="1.0.0")

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Global instances
model_registry = ModelRegistry()
feature_store = FeatureStore()
model_server = ModelServer(model_registry, feature_store)


@app.post("/predict", response_model=PredictionResponse)
async def predict(request: PredictionRequest):
    """Make a single prediction."""
    return await model_server.predict(request)


@app.post("/predict/batch", response_model=List[PredictionResponse])
async def predict_batch(request: BatchPredictionRequest):
    """Make batch predictions."""
    return await model_server.predict_batch(request)


@app.post("/models/register")
async def register_model(metadata_dict: Dict[str, Any]):
    """Register a new model."""
    try:
        metadata = ModelMetadata(
            model_id=metadata_dict["model_id"],
            model_name=metadata_dict["model_name"],
            model_type=metadata_dict["model_type"],
            version=metadata_dict["version"],
            problem_type=metadata_dict["problem_type"],
            input_schema=metadata_dict["input_schema"],
            output_schema=metadata_dict["output_schema"],
            model_path=metadata_dict["model_path"],
            feature_pipeline_path=metadata_dict.get("feature_pipeline_path")
        )
        
        success = await model_registry.register_model(metadata)
        
        if success:
            return {"status": "success", "message": "Model registered successfully"}
        else:
            raise HTTPException(status_code=500, detail="Failed to register model")
            
    except Exception as e:
        logger.error(f"Error registering model: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/models/{model_id}/load")
async def load_model(model_id: str):
    """Load a model into memory."""
    try:
        success = await model_registry.load_model(model_id)
        
        if success:
            return {"status": "success", "message": "Model loaded successfully"}
        else:
            raise HTTPException(status_code=404, detail="Model not found or failed to load")
            
    except Exception as e:
        logger.error(f"Error loading model {model_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.delete("/models/{model_id}/unload")
async def unload_model(model_id: str):
    """Unload a model from memory."""
    try:
        success = await model_registry.unload_model(model_id)
        
        if success:
            return {"status": "success", "message": "Model unloaded successfully"}
        else:
            raise HTTPException(status_code=404, detail="Model not found")
            
    except Exception as e:
        logger.error(f"Error unloading model {model_id}: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/models")
async def list_models():
    """List all registered models."""
    models = []
    for model_id, metadata in model_registry.models.items():
        model_info = {
            "model_id": model_id,
            "model_name": metadata.model_name,
            "model_type": metadata.model_type,
            "version": metadata.version,
            "problem_type": metadata.problem_type,
            "created_at": metadata.created_at,
            "last_used": metadata.last_used,
            "usage_count": metadata.usage_count,
            "avg_latency_ms": metadata.avg_latency_ms,
            "loaded": model_id in model_registry.loaded_models
        }
        models.append(model_info)
    
    return {"models": models}


@app.get("/models/{model_id}")
async def get_model_info(model_id: str):
    """Get information about a specific model."""
    metadata = model_registry.get_metadata(model_id)
    
    if not metadata:
        raise HTTPException(status_code=404, detail="Model not found")
    
    return {
        "model_id": model_id,
        "model_name": metadata.model_name,
        "model_type": metadata.model_type,
        "version": metadata.version,
        "problem_type": metadata.problem_type,
        "input_schema": metadata.input_schema,
        "output_schema": metadata.output_schema,
        "created_at": metadata.created_at,
        "last_used": metadata.last_used,
        "usage_count": metadata.usage_count,
        "avg_latency_ms": metadata.avg_latency_ms,
        "loaded": model_id in model_registry.loaded_models
    }


@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow(),
        "loaded_models": len(model_registry.loaded_models),
        "registered_models": len(model_registry.models)
    }


if __name__ == "__main__":
    uvicorn.run(
        "src.ml.deployment.model_server:app",
        host="0.0.0.0",
        port=8001,
        reload=True
    )