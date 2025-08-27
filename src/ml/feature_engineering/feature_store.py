"""
Feature Store Implementation

Centralized feature store for managing, versioning, and serving features
across ML pipelines with real-time and batch capabilities.
"""

import logging
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Any, Union, Tuple
from datetime import datetime, timedelta
from dataclasses import dataclass, field
from abc import ABC, abstractmethod
from pathlib import Path
import json
import hashlib
import asyncio
import aioredis
from sqlalchemy import create_engine, MetaData, Table, Column, String, DateTime, Text, JSON
from sqlalchemy.dialects.postgresql import UUID
import uuid

from src.core.config import get_settings
from src.core.logging import get_logger
from src.data_access.supabase_client import get_supabase_client
from src.core.caching.redis_cache import get_redis_client
from src.core.monitoring.metrics import MetricsCollector

logger = get_logger(__name__)
settings = get_settings()


@dataclass
class FeatureSchema:
    """Schema definition for features."""
    
    name: str
    version: str
    description: str
    feature_type: str  # "numerical", "categorical", "text", "datetime"
    data_type: str  # "float", "int", "string", "timestamp"
    nullable: bool = True
    default_value: Optional[Any] = None
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    categories: Optional[List[str]] = None
    tags: List[str] = field(default_factory=list)
    owner: str = "system"
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)


@dataclass
class FeatureGroup:
    """Group of related features."""
    
    name: str
    version: str
    description: str
    features: List[FeatureSchema]
    entity_keys: List[str]  # Primary keys for joining
    source_table: Optional[str] = None
    transformation_logic: Optional[str] = None
    refresh_interval: Optional[str] = "daily"  # "hourly", "daily", "weekly"
    tags: List[str] = field(default_factory=list)
    owner: str = "system"
    created_at: datetime = field(default_factory=datetime.utcnow)
    updated_at: datetime = field(default_factory=datetime.utcnow)


class FeatureStore:
    """Centralized feature store with versioning and serving capabilities."""
    
    def __init__(self):
        self.supabase = get_supabase_client()
        self.redis_client = get_redis_client()
        self.metrics_collector = MetricsCollector()
        self._initialize_tables()
        
    def _initialize_tables(self):
        """Initialize feature store tables."""
        try:
            # Feature schemas table
            self.supabase.table('feature_schemas').select("*").limit(1).execute()
        except:
            # Create tables if they don't exist
            logger.info("Creating feature store tables")
            self._create_tables()
    
    def _create_tables(self):
        """Create feature store tables."""
        try:
            # Create feature_groups table
            self.supabase.rpc('create_feature_store_tables', {}).execute()
            logger.info("Feature store tables created successfully")
        except Exception as e:
            logger.warning(f"Could not create feature store tables: {str(e)}")
            # Tables might already exist, which is okay
    
    async def register_feature_group(self, feature_group: FeatureGroup) -> str:
        """Register a new feature group."""
        try:
            feature_group_id = str(uuid.uuid4())
            
            # Store feature group metadata
            feature_group_data = {
                "id": feature_group_id,
                "name": feature_group.name,
                "version": feature_group.version,
                "description": feature_group.description,
                "entity_keys": feature_group.entity_keys,
                "source_table": feature_group.source_table,
                "transformation_logic": feature_group.transformation_logic,
                "refresh_interval": feature_group.refresh_interval,
                "tags": feature_group.tags,
                "owner": feature_group.owner,
                "created_at": feature_group.created_at.isoformat(),
                "updated_at": feature_group.updated_at.isoformat()
            }
            
            result = self.supabase.table('feature_groups').insert(feature_group_data).execute()
            
            # Register individual features
            for feature in feature_group.features:
                await self._register_feature_schema(feature, feature_group_id)
            
            logger.info(f"Registered feature group: {feature_group.name} v{feature_group.version}")
            
            self.metrics_collector.increment_counter(
                "feature_store_feature_groups_registered_total",
                tags={"name": feature_group.name, "version": feature_group.version}
            )
            
            return feature_group_id
            
        except Exception as e:
            logger.error(f"Error registering feature group: {str(e)}")
            raise
    
    async def _register_feature_schema(self, feature: FeatureSchema, feature_group_id: str):
        """Register a feature schema."""
        feature_data = {
            "id": str(uuid.uuid4()),
            "feature_group_id": feature_group_id,
            "name": feature.name,
            "version": feature.version,
            "description": feature.description,
            "feature_type": feature.feature_type,
            "data_type": feature.data_type,
            "nullable": feature.nullable,
            "default_value": feature.default_value,
            "min_value": feature.min_value,
            "max_value": feature.max_value,
            "categories": feature.categories,
            "tags": feature.tags,
            "owner": feature.owner,
            "created_at": feature.created_at.isoformat(),
            "updated_at": feature.updated_at.isoformat()
        }
        
        self.supabase.table('feature_schemas').insert(feature_data).execute()
    
    async def ingest_features(self, feature_group_name: str, version: str, 
                            features_df: pd.DataFrame) -> bool:
        """Ingest feature values into the feature store."""
        try:
            start_time = datetime.utcnow()
            
            # Get feature group metadata
            feature_group = await self.get_feature_group(feature_group_name, version)
            if not feature_group:
                raise ValueError(f"Feature group {feature_group_name} v{version} not found")
            
            # Validate feature schema
            await self._validate_features(features_df, feature_group)
            
            # Store features in batch table
            batch_id = str(uuid.uuid4())
            timestamp = datetime.utcnow()
            
            # Convert DataFrame to records
            records = []
            for _, row in features_df.iterrows():
                record = {
                    "batch_id": batch_id,
                    "feature_group_name": feature_group_name,
                    "version": version,
                    "timestamp": timestamp.isoformat(),
                    "entity_keys": {key: row.get(key) for key in feature_group['entity_keys']},
                    "features": row.to_dict()
                }
                records.append(record)
            
            # Batch insert
            self.supabase.table('feature_values').insert(records).execute()
            
            # Cache latest features for real-time serving
            await self._cache_latest_features(feature_group_name, version, features_df)
            
            duration = (datetime.utcnow() - start_time).total_seconds()
            logger.info(f"Ingested {len(features_df)} feature records in {duration:.2f}s")
            
            self.metrics_collector.increment_counter(
                "feature_store_features_ingested_total",
                tags={"feature_group": feature_group_name, "version": version}
            )
            self.metrics_collector.record_histogram(
                "feature_store_ingestion_duration_seconds",
                duration
            )
            
            return True
            
        except Exception as e:
            logger.error(f"Error ingesting features: {str(e)}")
            self.metrics_collector.increment_counter(
                "feature_store_ingestion_errors_total",
                tags={"feature_group": feature_group_name, "version": version}
            )
            raise
    
    async def get_features(self, feature_group_name: str, version: str,
                          entity_keys: Dict[str, Any],
                          features: Optional[List[str]] = None,
                          timestamp: Optional[datetime] = None) -> Optional[Dict[str, Any]]:
        """Get features for specific entities."""
        try:
            # Try cache first for real-time serving
            cache_key = self._generate_cache_key(feature_group_name, version, entity_keys)
            cached_features = await self.redis_client.get(cache_key)
            
            if cached_features:
                cached_data = json.loads(cached_features)
                if not timestamp or datetime.fromisoformat(cached_data['timestamp']) >= timestamp:
                    return cached_data['features']
            
            # Query from database
            query = self.supabase.table('feature_values').select('*')
            query = query.eq('feature_group_name', feature_group_name)
            query = query.eq('version', version)
            
            # Filter by entity keys
            for key, value in entity_keys.items():
                query = query.eq(f'entity_keys->{key}', value)
            
            if timestamp:
                query = query.lte('timestamp', timestamp.isoformat())
            
            # Get latest record
            result = query.order('timestamp', desc=True).limit(1).execute()
            
            if result.data:
                record = result.data[0]
                feature_data = record['features']
                
                # Filter specific features if requested
                if features:
                    feature_data = {k: v for k, v in feature_data.items() if k in features}
                
                # Cache for future requests
                cache_data = {
                    'features': feature_data,
                    'timestamp': record['timestamp']
                }
                await self.redis_client.setex(
                    cache_key, 
                    300,  # 5 minutes TTL
                    json.dumps(cache_data, default=str)
                )
                
                return feature_data
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting features: {str(e)}")
            raise
    
    async def get_features_batch(self, feature_group_name: str, version: str,
                               entity_keys_list: List[Dict[str, Any]],
                               features: Optional[List[str]] = None,
                               timestamp: Optional[datetime] = None) -> pd.DataFrame:
        """Get features for multiple entities in batch."""
        try:
            start_time = datetime.utcnow()
            
            results = []
            for entity_keys in entity_keys_list:
                feature_data = await self.get_features(
                    feature_group_name, version, entity_keys, features, timestamp
                )
                if feature_data:
                    # Add entity keys to result
                    result_row = {**entity_keys, **feature_data}
                    results.append(result_row)
            
            df = pd.DataFrame(results)
            
            duration = (datetime.utcnow() - start_time).total_seconds()
            logger.info(f"Retrieved {len(results)} feature records in {duration:.2f}s")
            
            self.metrics_collector.record_histogram(
                "feature_store_batch_retrieval_duration_seconds",
                duration
            )
            
            return df
            
        except Exception as e:
            logger.error(f"Error getting batch features: {str(e)}")
            raise
    
    async def get_feature_group(self, name: str, version: str) -> Optional[Dict[str, Any]]:
        """Get feature group metadata."""
        try:
            result = self.supabase.table('feature_groups').select('*').eq('name', name).eq('version', version).execute()
            
            if result.data:
                return result.data[0]
            return None
            
        except Exception as e:
            logger.error(f"Error getting feature group: {str(e)}")
            raise
    
    async def list_feature_groups(self, tags: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """List available feature groups."""
        try:
            query = self.supabase.table('feature_groups').select('*')
            
            if tags:
                # Filter by tags (this would need proper JSON querying)
                pass
            
            result = query.execute()
            return result.data
            
        except Exception as e:
            logger.error(f"Error listing feature groups: {str(e)}")
            raise
    
    async def get_feature_statistics(self, feature_group_name: str, version: str,
                                   start_date: datetime, end_date: datetime) -> Dict[str, Any]:
        """Get feature statistics over time period."""
        try:
            # This would typically involve complex aggregations
            # For now, return basic stats
            query = self.supabase.table('feature_values').select('features')
            query = query.eq('feature_group_name', feature_group_name)
            query = query.eq('version', version)
            query = query.gte('timestamp', start_date.isoformat())
            query = query.lte('timestamp', end_date.isoformat())
            
            result = query.execute()
            
            if not result.data:
                return {}
            
            # Aggregate statistics
            all_features = []
            for record in result.data:
                all_features.append(record['features'])
            
            df = pd.DataFrame(all_features)
            
            stats = {}
            for column in df.columns:
                if df[column].dtype in ['int64', 'float64']:
                    stats[column] = {
                        'mean': float(df[column].mean()),
                        'std': float(df[column].std()),
                        'min': float(df[column].min()),
                        'max': float(df[column].max()),
                        'count': int(df[column].count()),
                        'null_count': int(df[column].isnull().sum())
                    }
                else:
                    value_counts = df[column].value_counts().head(10)
                    stats[column] = {
                        'unique_count': int(df[column].nunique()),
                        'null_count': int(df[column].isnull().sum()),
                        'top_values': value_counts.to_dict()
                    }
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting feature statistics: {str(e)}")
            raise
    
    async def _validate_features(self, features_df: pd.DataFrame, feature_group: Dict[str, Any]):
        """Validate feature values against schema."""
        # Get feature schemas
        schemas_result = self.supabase.table('feature_schemas').select('*').eq(
            'feature_group_id', feature_group['id']
        ).execute()
        
        feature_schemas = {schema['name']: schema for schema in schemas_result.data}
        
        # Validate each feature
        for column in features_df.columns:
            if column in feature_schemas:
                schema = feature_schemas[column]
                
                # Check data type compatibility
                if schema['data_type'] == 'float' and not pd.api.types.is_numeric_dtype(features_df[column]):
                    logger.warning(f"Feature {column} should be numeric but found {features_df[column].dtype}")
                
                # Check null values
                if not schema['nullable'] and features_df[column].isnull().any():
                    raise ValueError(f"Feature {column} contains null values but is not nullable")
                
                # Check value ranges for numerical features
                if schema['min_value'] is not None:
                    if features_df[column].min() < schema['min_value']:
                        logger.warning(f"Feature {column} has values below minimum {schema['min_value']}")
                
                if schema['max_value'] is not None:
                    if features_df[column].max() > schema['max_value']:
                        logger.warning(f"Feature {column} has values above maximum {schema['max_value']}")
                
                # Check categories for categorical features
                if schema['categories'] and schema['feature_type'] == 'categorical':
                    invalid_categories = set(features_df[column].dropna().unique()) - set(schema['categories'])
                    if invalid_categories:
                        logger.warning(f"Feature {column} has invalid categories: {invalid_categories}")
    
    async def _cache_latest_features(self, feature_group_name: str, version: str, features_df: pd.DataFrame):
        """Cache latest features for real-time serving."""
        try:
            # Get feature group to know entity keys
            feature_group = await self.get_feature_group(feature_group_name, version)
            entity_keys = feature_group['entity_keys']
            
            # Cache each row
            for _, row in features_df.iterrows():
                entity_key_values = {key: row[key] for key in entity_keys if key in row}
                cache_key = self._generate_cache_key(feature_group_name, version, entity_key_values)
                
                cache_data = {
                    'features': row.to_dict(),
                    'timestamp': datetime.utcnow().isoformat()
                }
                
                await self.redis_client.setex(
                    cache_key,
                    300,  # 5 minutes TTL
                    json.dumps(cache_data, default=str)
                )
                
        except Exception as e:
            logger.error(f"Error caching features: {str(e)}")
    
    def _generate_cache_key(self, feature_group_name: str, version: str, entity_keys: Dict[str, Any]) -> str:
        """Generate cache key for feature lookup."""
        key_str = f"{feature_group_name}:{version}:{json.dumps(entity_keys, sort_keys=True)}"
        return f"features:{hashlib.md5(key_str.encode()).hexdigest()}"


class FeatureStoreClient:
    """Client for accessing feature store."""
    
    def __init__(self, feature_store: FeatureStore):
        self.feature_store = feature_store
    
    async def get_training_dataset(self, feature_groups: List[Dict[str, str]],
                                 entity_keys: List[Dict[str, Any]],
                                 timestamp: Optional[datetime] = None) -> pd.DataFrame:
        """Get training dataset from multiple feature groups."""
        datasets = []
        
        for fg_config in feature_groups:
            name = fg_config['name']
            version = fg_config['version']
            features = fg_config.get('features')
            
            df = await self.feature_store.get_features_batch(
                name, version, entity_keys, features, timestamp
            )
            
            if not datasets:
                datasets.append(df)
            else:
                # Join with previous datasets
                join_keys = list(entity_keys[0].keys())
                datasets[0] = datasets[0].merge(df, on=join_keys, how='inner')
        
        return datasets[0] if datasets else pd.DataFrame()
    
    async def get_online_features(self, feature_groups: List[Dict[str, str]],
                                entity_keys: Dict[str, Any]) -> Dict[str, Any]:
        """Get features for online serving."""
        all_features = {}
        
        for fg_config in feature_groups:
            name = fg_config['name']
            version = fg_config['version']
            features = fg_config.get('features')
            
            feature_data = await self.feature_store.get_features(
                name, version, entity_keys, features
            )
            
            if feature_data:
                all_features.update(feature_data)
        
        return all_features


# Predefined feature groups for sales domain
async def create_sales_feature_groups(feature_store: FeatureStore):
    """Create predefined feature groups for sales domain."""
    
    # Customer features
    customer_features = [
        FeatureSchema(
            name="total_orders",
            version="1.0",
            description="Total number of orders by customer",
            feature_type="numerical",
            data_type="int",
            nullable=False,
            min_value=0
        ),
        FeatureSchema(
            name="total_spent",
            version="1.0",
            description="Total amount spent by customer",
            feature_type="numerical",
            data_type="float",
            nullable=False,
            min_value=0.0
        ),
        FeatureSchema(
            name="avg_order_value",
            version="1.0",
            description="Average order value for customer",
            feature_type="numerical",
            data_type="float",
            nullable=False,
            min_value=0.0
        ),
        FeatureSchema(
            name="days_since_last_order",
            version="1.0",
            description="Days since customer's last order",
            feature_type="numerical",
            data_type="int",
            nullable=True,
            min_value=0
        ),
        FeatureSchema(
            name="customer_segment",
            version="1.0",
            description="Customer segment based on RFM analysis",
            feature_type="categorical",
            data_type="string",
            nullable=False,
            categories=["High_Value", "Medium_Value", "Low_Value", "At_Risk", "New"]
        )
    ]
    
    customer_feature_group = FeatureGroup(
        name="customer_features",
        version="1.0",
        description="Customer behavioral and transactional features",
        features=customer_features,
        entity_keys=["CustomerID"],
        source_table="gold_sales",
        refresh_interval="daily",
        tags=["customer", "behavioral", "transactional"]
    )
    
    await feature_store.register_feature_group(customer_feature_group)
    
    # Product features
    product_features = [
        FeatureSchema(
            name="total_quantity_sold",
            version="1.0",
            description="Total quantity sold for product",
            feature_type="numerical",
            data_type="int",
            nullable=False,
            min_value=0
        ),
        FeatureSchema(
            name="total_revenue",
            version="1.0",
            description="Total revenue generated by product",
            feature_type="numerical",
            data_type="float",
            nullable=False,
            min_value=0.0
        ),
        FeatureSchema(
            name="avg_unit_price",
            version="1.0",
            description="Average unit price for product",
            feature_type="numerical",
            data_type="float",
            nullable=False,
            min_value=0.0
        ),
        FeatureSchema(
            name="unique_customers",
            version="1.0",
            description="Number of unique customers who bought product",
            feature_type="numerical",
            data_type="int",
            nullable=False,
            min_value=0
        )
    ]
    
    product_feature_group = FeatureGroup(
        name="product_features",
        version="1.0",
        description="Product performance and popularity features",
        features=product_features,
        entity_keys=["StockCode"],
        source_table="gold_sales",
        refresh_interval="daily",
        tags=["product", "performance", "popularity"]
    )
    
    await feature_store.register_feature_group(product_feature_group)
    
    # Time-based features
    time_features = [
        FeatureSchema(
            name="day_of_week",
            version="1.0",
            description="Day of week (0=Monday, 6=Sunday)",
            feature_type="categorical",
            data_type="int",
            nullable=False,
            categories=["0", "1", "2", "3", "4", "5", "6"]
        ),
        FeatureSchema(
            name="month",
            version="1.0",
            description="Month of the year (1-12)",
            feature_type="categorical",
            data_type="int",
            nullable=False,
            min_value=1,
            max_value=12
        ),
        FeatureSchema(
            name="is_weekend",
            version="1.0",
            description="Whether the date is a weekend",
            feature_type="categorical",
            data_type="int",
            nullable=False,
            categories=["0", "1"]
        ),
        FeatureSchema(
            name="is_holiday",
            version="1.0",
            description="Whether the date is a holiday",
            feature_type="categorical",
            data_type="int",
            nullable=False,
            categories=["0", "1"]
        )
    ]
    
    time_feature_group = FeatureGroup(
        name="time_features",
        version="1.0",
        description="Time-based features for temporal analysis",
        features=time_features,
        entity_keys=["date"],
        refresh_interval="daily",
        tags=["time", "temporal", "calendar"]
    )
    
    await feature_store.register_feature_group(time_feature_group)
    
    logger.info("Sales feature groups created successfully")


class FeatureLineage:
    """Feature lineage tracking system."""
    
    def __init__(self, feature_store: FeatureStore):
        self.feature_store = feature_store
        self.supabase = feature_store.supabase
    
    async def track_feature_lineage(self, feature_name: str, source_features: List[str],
                                  transformation: str, metadata: Dict[str, Any] = None):
        """Track lineage for a derived feature."""
        try:
            lineage_record = {
                "id": str(uuid.uuid4()),
                "feature_name": feature_name,
                "source_features": source_features,
                "transformation": transformation,
                "metadata": metadata or {},
                "created_at": datetime.utcnow().isoformat()
            }
            
            self.supabase.table('feature_lineage').insert(lineage_record).execute()
            logger.info(f"Tracked lineage for feature: {feature_name}")
            
        except Exception as e:
            logger.error(f"Error tracking feature lineage: {str(e)}")
    
    async def get_feature_lineage(self, feature_name: str, depth: int = 5) -> Dict[str, Any]:
        """Get feature lineage graph."""
        try:
            def get_upstream_features(feature: str, current_depth: int) -> Dict[str, Any]:
                if current_depth >= depth:
                    return {"feature": feature, "sources": []}
                
                # Get direct upstream features
                result = self.supabase.table('feature_lineage').select('*').eq('feature_name', feature).execute()
                
                sources = []
                for record in result.data:
                    for source_feature in record['source_features']:
                        source_lineage = get_upstream_features(source_feature, current_depth + 1)
                        sources.append({
                            "feature": source_feature,
                            "transformation": record['transformation'],
                            "metadata": record['metadata'],
                            "upstream": source_lineage
                        })
                
                return {"feature": feature, "sources": sources}
            
            lineage = get_upstream_features(feature_name, 0)
            
            # Get downstream features
            downstream_result = self.supabase.table('feature_lineage').select('*').contains('source_features', [feature_name]).execute()
            
            downstream = []
            for record in downstream_result.data:
                downstream.append({
                    "feature": record['feature_name'],
                    "transformation": record['transformation']
                })
            
            return {
                "upstream": lineage,
                "downstream": downstream,
                "affected_models": [],  # Would be populated from model dependencies
                "affected_pipelines": [],  # Would be populated from pipeline dependencies
                "dependency_count": len(lineage.get("sources", [])),
                "impact_score": self._calculate_impact_score(downstream)
            }
            
        except Exception as e:
            logger.error(f"Error getting feature lineage: {str(e)}")
            return {}
    
    def _calculate_impact_score(self, downstream_features: List[Dict[str, Any]]) -> float:
        """Calculate impact score based on downstream dependencies."""
        if not downstream_features:
            return 0.0
        
        # Simple scoring based on number of downstream features
        # In practice, this would consider model importance, usage frequency, etc.
        return min(1.0, len(downstream_features) / 10.0)


class FeatureValidation:
    """Feature validation and quality assessment."""
    
    def __init__(self, feature_store: FeatureStore):
        self.feature_store = feature_store
    
    async def validate_feature_quality(self, feature_group_name: str, version: str,
                                     quality_rules: Dict[str, Any]) -> Dict[str, Any]:
        """Validate feature quality against defined rules."""
        try:
            # Get recent feature data
            end_date = datetime.utcnow()
            start_date = end_date - timedelta(days=7)  # Last 7 days
            
            stats = await self.feature_store.get_feature_statistics(
                feature_group_name, version, start_date, end_date
            )
            
            quality_results = {
                "feature_group": feature_group_name,
                "version": version,
                "validation_date": datetime.utcnow().isoformat(),
                "overall_score": 0.0,
                "feature_scores": {},
                "issues": []
            }
            
            total_score = 0.0
            feature_count = 0
            
            for feature_name, feature_stats in stats.items():
                feature_score = 1.0
                feature_issues = []
                
                # Check completeness
                if 'null_count' in feature_stats and 'count' in feature_stats:
                    null_rate = feature_stats['null_count'] / (feature_stats['count'] + feature_stats['null_count'])
                    if null_rate > quality_rules.get('max_null_rate', 0.1):
                        feature_score -= 0.3
                        feature_issues.append(f"High null rate: {null_rate:.2%}")
                
                # Check value ranges for numerical features
                if 'mean' in feature_stats:
                    if quality_rules.get('expected_ranges'):
                        expected_range = quality_rules['expected_ranges'].get(feature_name)
                        if expected_range:
                            min_val, max_val = expected_range
                            if feature_stats['min'] < min_val or feature_stats['max'] > max_val:
                                feature_score -= 0.2
                                feature_issues.append(f"Values outside expected range [{min_val}, {max_val}]")
                
                # Check uniqueness for categorical features
                if 'unique_count' in feature_stats:
                    if quality_rules.get('min_unique_values'):
                        min_unique = quality_rules['min_unique_values'].get(feature_name, 1)
                        if feature_stats['unique_count'] < min_unique:
                            feature_score -= 0.2
                            feature_issues.append(f"Low uniqueness: {feature_stats['unique_count']} unique values")
                
                quality_results['feature_scores'][feature_name] = max(0.0, feature_score)
                if feature_issues:
                    quality_results['issues'].extend([f"{feature_name}: {issue}" for issue in feature_issues])
                
                total_score += feature_score
                feature_count += 1
            
            quality_results['overall_score'] = total_score / feature_count if feature_count > 0 else 0.0
            
            return quality_results
            
        except Exception as e:
            logger.error(f"Error validating feature quality: {str(e)}")
            return {"error": str(e)}
    
    async def detect_feature_drift(self, feature_group_name: str, version: str,
                                 reference_period_days: int = 30,
                                 comparison_period_days: int = 7) -> Dict[str, Any]:
        """Detect feature drift by comparing recent data to reference period."""
        try:
            end_date = datetime.utcnow()
            comparison_start = end_date - timedelta(days=comparison_period_days)
            reference_end = comparison_start
            reference_start = reference_end - timedelta(days=reference_period_days)
            
            # Get statistics for both periods
            reference_stats = await self.feature_store.get_feature_statistics(
                feature_group_name, version, reference_start, reference_end
            )
            
            comparison_stats = await self.feature_store.get_feature_statistics(
                feature_group_name, version, comparison_start, end_date
            )
            
            drift_results = {
                "feature_group": feature_group_name,
                "version": version,
                "reference_period": {"start": reference_start.isoformat(), "end": reference_end.isoformat()},
                "comparison_period": {"start": comparison_start.isoformat(), "end": end_date.isoformat()},
                "drift_detected": False,
                "feature_drifts": {}
            }
            
            for feature_name in reference_stats:
                if feature_name not in comparison_stats:
                    continue
                
                ref_stats = reference_stats[feature_name]
                comp_stats = comparison_stats[feature_name]
                
                drift_score = 0.0
                drift_indicators = []
                
                # For numerical features
                if 'mean' in ref_stats and 'mean' in comp_stats:
                    mean_drift = abs(comp_stats['mean'] - ref_stats['mean']) / (ref_stats['std'] + 1e-8)
                    if mean_drift > 2.0:  # More than 2 standard deviations
                        drift_score += 0.5
                        drift_indicators.append(f"Mean drift: {mean_drift:.2f} std devs")
                    
                    std_drift = abs(comp_stats['std'] - ref_stats['std']) / (ref_stats['std'] + 1e-8)
                    if std_drift > 0.5:  # More than 50% change in std
                        drift_score += 0.3
                        drift_indicators.append(f"Std deviation drift: {std_drift:.2%}")
                
                # For categorical features
                if 'top_values' in ref_stats and 'top_values' in comp_stats:
                    # Compare distribution of top values
                    ref_dist = ref_stats['top_values']
                    comp_dist = comp_stats['top_values']
                    
                    # Calculate Jensen-Shannon divergence (simplified)
                    all_values = set(ref_dist.keys()) | set(comp_dist.keys())
                    js_divergence = self._calculate_js_divergence(ref_dist, comp_dist, all_values)
                    
                    if js_divergence > 0.1:
                        drift_score += js_divergence
                        drift_indicators.append(f"Distribution drift: {js_divergence:.3f} JS divergence")
                
                drift_results['feature_drifts'][feature_name] = {
                    "drift_score": drift_score,
                    "is_drifted": drift_score > 0.1,
                    "indicators": drift_indicators
                }
                
                if drift_score > 0.1:
                    drift_results['drift_detected'] = True
            
            return drift_results
            
        except Exception as e:
            logger.error(f"Error detecting feature drift: {str(e)}")
            return {"error": str(e)}
    
    def _calculate_js_divergence(self, dist1: Dict[str, float], dist2: Dict[str, float],
                               all_values: set) -> float:
        """Calculate Jensen-Shannon divergence between two distributions."""
        # Normalize distributions
        total1 = sum(dist1.values())
        total2 = sum(dist2.values())
        
        p = [dist1.get(v, 0) / total1 for v in all_values]
        q = [dist2.get(v, 0) / total2 for v in all_values]
        
        # Add small epsilon to avoid log(0)
        p = [x + 1e-8 for x in p]
        q = [x + 1e-8 for x in q]
        
        # Calculate JS divergence
        m = [(p[i] + q[i]) / 2 for i in range(len(p))]
        
        kl_pm = sum(p[i] * np.log(p[i] / m[i]) for i in range(len(p)))
        kl_qm = sum(q[i] * np.log(q[i] / m[i]) for i in range(len(q)))
        
        return (kl_pm + kl_qm) / 2


# Additional utility functions
async def save_features(feature_store: FeatureStore, pipeline_name: str,
                       features: Dict[str, Any], metadata: Dict[str, Any] = None):
    """Save features generated by a pipeline."""
    try:
        # This would integrate with the pipeline execution
        logger.info(f"Saving features from pipeline: {pipeline_name}")
        
        # Convert features to DataFrame
        df = pd.DataFrame([features])
        
        # Ingest into appropriate feature group based on pipeline
        if "customer" in pipeline_name.lower():
            await feature_store.ingest_features("customer_features", "1.0", df)
        elif "product" in pipeline_name.lower():
            await feature_store.ingest_features("product_features", "1.0", df)
        else:
            # Create or use a generic feature group
            logger.info(f"Using generic feature group for pipeline: {pipeline_name}")
        
    except Exception as e:
        logger.error(f"Error saving features: {str(e)}")
        raise


def create_feature_store() -> FeatureStore:
    """Factory function to create a feature store instance."""
    return FeatureStore()


def create_feature_store_client(feature_store: FeatureStore) -> FeatureStoreClient:
    """Factory function to create a feature store client."""
    return FeatureStoreClient(feature_store)


def create_feature_lineage(feature_store: FeatureStore) -> FeatureLineage:
    """Factory function to create a feature lineage tracker."""
    return FeatureLineage(feature_store)


def create_feature_validation(feature_store: FeatureStore) -> FeatureValidation:
    """Factory function to create a feature validation system."""
    return FeatureValidation(feature_store)