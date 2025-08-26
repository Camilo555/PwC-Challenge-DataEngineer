"""
Real-time Data Transformation and Enrichment Pipelines
Provides streaming ETL capabilities with data enrichment, validation, and transformation.
"""
from __future__ import annotations

import asyncio
import json
import threading
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Callable, Dict, List, Optional, Union

import requests
from kafka import KafkaConsumer, KafkaProducer

from core.logging import get_logger
from external_apis.enrichment_service import EnrichmentService
from monitoring.advanced_metrics import get_metrics_collector
from src.domain.validators.data_quality import DataQualityValidator
from src.streaming.kafka_manager import KafkaManager, StreamingMessage, StreamingTopic


class TransformationType(Enum):
    """Types of transformations"""
    CLEANSING = "cleansing"
    VALIDATION = "validation"
    ENRICHMENT = "enrichment"
    AGGREGATION = "aggregation"
    FILTERING = "filtering"
    FORMATTING = "formatting"
    STANDARDIZATION = "standardization"


class EnrichmentSource(Enum):
    """External enrichment sources"""
    CUSTOMER_API = "customer_api"
    PRODUCT_API = "product_api"
    CURRENCY_API = "currency_api"
    LOCATION_API = "location_api"
    CACHE = "cache"
    DATABASE = "database"


@dataclass
class TransformationRule:
    """Data transformation rule definition"""
    rule_id: str
    name: str
    transformation_type: TransformationType
    source_fields: List[str]
    target_fields: List[str]
    conditions: Dict[str, Any]
    transformation_logic: str
    priority: int = 1
    enabled: bool = True
    created_at: datetime = field(default_factory=datetime.now)


@dataclass
class EnrichmentRule:
    """Data enrichment rule definition"""
    rule_id: str
    name: str
    source: EnrichmentSource
    lookup_field: str
    target_fields: List[str]
    cache_ttl_seconds: int = 3600
    fallback_value: Any = None
    timeout_ms: int = 5000
    enabled: bool = True
    created_at: datetime = field(default_factory=datetime.now)


@dataclass
class TransformationResult:
    """Result of data transformation"""
    success: bool
    original_data: Dict[str, Any]
    transformed_data: Dict[str, Any]
    applied_rules: List[str]
    enrichments: Dict[str, Any]
    validation_errors: List[str]
    processing_time_ms: float
    timestamp: datetime = field(default_factory=datetime.now)


class DataTransformer(ABC):
    """Abstract base class for data transformers"""
    
    @abstractmethod
    def transform(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform data"""
        pass
        
    @abstractmethod
    def validate(self, data: Dict[str, Any]) -> List[str]:
        """Validate data and return errors"""
        pass


class StreamingDataTransformer(DataTransformer):
    """
    Streaming data transformer with real-time cleansing and validation
    """
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.transformation_rules: Dict[str, TransformationRule] = {}
        self.data_quality_validator = DataQualityValidator()
        self.metrics_collector = get_metrics_collector()
        
    def add_transformation_rule(self, rule: TransformationRule):
        """Add transformation rule"""
        self.transformation_rules[rule.rule_id] = rule
        self.logger.info(f"Added transformation rule: {rule.name}")
        
    def transform(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Transform data using configured rules"""
        transformed_data = data.copy()
        
        # Apply transformation rules by priority
        sorted_rules = sorted(
            self.transformation_rules.values(),
            key=lambda r: r.priority
        )
        
        for rule in sorted_rules:
            if not rule.enabled:
                continue
                
            try:
                if self._rule_conditions_met(rule, transformed_data):
                    transformed_data = self._apply_transformation_rule(rule, transformed_data)
            except Exception as e:
                self.logger.error(f"Transformation rule {rule.rule_id} failed: {e}")
                
        return transformed_data
        
    def validate(self, data: Dict[str, Any]) -> List[str]:
        """Validate data using quality validator"""
        try:
            validation_result = self.data_quality_validator.validate_record(data)
            return validation_result.get('errors', [])
        except Exception as e:
            self.logger.error(f"Data validation failed: {e}")
            return [f"Validation error: {str(e)}"]
            
    def _rule_conditions_met(self, rule: TransformationRule, data: Dict[str, Any]) -> bool:
        """Check if rule conditions are met"""
        conditions = rule.conditions
        
        if not conditions:
            return True
            
        # Check field existence conditions
        required_fields = conditions.get('required_fields', [])
        for field in required_fields:
            if not self._get_nested_value(data, field):
                return False
                
        # Check field value conditions
        field_conditions = conditions.get('field_conditions', {})
        for field, condition in field_conditions.items():
            value = self._get_nested_value(data, field)
            if not self._check_field_condition(value, condition):
                return False
                
        return True
        
    def _apply_transformation_rule(self, rule: TransformationRule, data: Dict[str, Any]) -> Dict[str, Any]:
        """Apply specific transformation rule"""
        transformed_data = data.copy()
        
        try:
            if rule.transformation_type == TransformationType.CLEANSING:
                transformed_data = self._apply_cleansing(rule, transformed_data)
            elif rule.transformation_type == TransformationType.VALIDATION:
                transformed_data = self._apply_validation(rule, transformed_data)
            elif rule.transformation_type == TransformationType.FORMATTING:
                transformed_data = self._apply_formatting(rule, transformed_data)
            elif rule.transformation_type == TransformationType.STANDARDIZATION:
                transformed_data = self._apply_standardization(rule, transformed_data)
            elif rule.transformation_type == TransformationType.FILTERING:
                transformed_data = self._apply_filtering(rule, transformed_data)
                
        except Exception as e:
            self.logger.error(f"Failed to apply rule {rule.rule_id}: {e}")
            
        return transformed_data
        
    def _apply_cleansing(self, rule: TransformationRule, data: Dict[str, Any]) -> Dict[str, Any]:
        """Apply data cleansing transformations"""
        for field in rule.source_fields:
            value = self._get_nested_value(data, field)
            if value is not None:
                # Clean string values
                if isinstance(value, str):
                    # Remove extra whitespace
                    value = value.strip()
                    # Remove special characters if specified
                    if 'remove_special_chars' in rule.transformation_logic:
                        import re
                        value = re.sub(r'[^\w\s-]', '', value)
                    # Convert to proper case if specified
                    if 'proper_case' in rule.transformation_logic:
                        value = value.title()
                        
                # Set cleaned value
                self._set_nested_value(data, field, value)
                
        return data
        
    def _apply_validation(self, rule: TransformationRule, data: Dict[str, Any]) -> Dict[str, Any]:
        """Apply validation rules"""
        validation_logic = rule.transformation_logic
        
        for field in rule.source_fields:
            value = self._get_nested_value(data, field)
            
            if 'required' in validation_logic and not value:
                data.setdefault('_validation_errors', []).append(f"Required field missing: {field}")
                
            if 'email' in validation_logic and value:
                import re
                if not re.match(r'^[^\s@]+@[^\s@]+\.[^\s@]+$', str(value)):
                    data.setdefault('_validation_errors', []).append(f"Invalid email format: {field}")
                    
            if 'phone' in validation_logic and value:
                import re
                if not re.match(r'^\+?[\d\s\-\(\)]+$', str(value)):
                    data.setdefault('_validation_errors', []).append(f"Invalid phone format: {field}")
                    
        return data
        
    def _apply_formatting(self, rule: TransformationRule, data: Dict[str, Any]) -> Dict[str, Any]:
        """Apply formatting transformations"""
        formatting_logic = rule.transformation_logic
        
        for i, field in enumerate(rule.source_fields):
            value = self._get_nested_value(data, field)
            target_field = rule.target_fields[i] if i < len(rule.target_fields) else field
            
            if value is not None:
                if 'date_format' in formatting_logic:
                    # Format dates
                    if isinstance(value, str):
                        try:
                            from dateutil.parser import parse
                            dt = parse(value)
                            formatted_value = dt.strftime(formatting_logic['date_format'])
                            self._set_nested_value(data, target_field, formatted_value)
                        except:
                            pass
                            
                elif 'decimal_places' in formatting_logic:
                    # Format numbers
                    if isinstance(value, (int, float)):
                        places = formatting_logic['decimal_places']
                        formatted_value = round(float(value), places)
                        self._set_nested_value(data, target_field, formatted_value)
                        
        return data
        
    def _apply_standardization(self, rule: TransformationRule, data: Dict[str, Any]) -> Dict[str, Any]:
        """Apply standardization transformations"""
        standardization_logic = rule.transformation_logic
        
        for field in rule.source_fields:
            value = self._get_nested_value(data, field)
            
            if value is not None and isinstance(value, str):
                if 'uppercase' in standardization_logic:
                    value = value.upper()
                elif 'lowercase' in standardization_logic:
                    value = value.lower()
                elif 'normalize_whitespace' in standardization_logic:
                    import re
                    value = re.sub(r'\s+', ' ', value).strip()
                    
                self._set_nested_value(data, field, value)
                
        return data
        
    def _apply_filtering(self, rule: TransformationRule, data: Dict[str, Any]) -> Dict[str, Any]:
        """Apply filtering transformations"""
        # Mark data for filtering if conditions met
        filter_logic = rule.transformation_logic
        
        if 'exclude_if' in filter_logic:
            exclude_conditions = filter_logic['exclude_if']
            for condition in exclude_conditions:
                field = condition.get('field')
                operator = condition.get('operator', 'eq')
                value = condition.get('value')
                
                field_value = self._get_nested_value(data, field)
                
                if self._check_field_condition(field_value, {'operator': operator, 'value': value}):
                    data['_filtered'] = True
                    break
                    
        return data
        
    def _get_nested_value(self, data: Dict[str, Any], field_path: str) -> Any:
        """Get nested field value using dot notation"""
        keys = field_path.split('.')
        value = data
        
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return None
                
        return value
        
    def _set_nested_value(self, data: Dict[str, Any], field_path: str, value: Any):
        """Set nested field value using dot notation"""
        keys = field_path.split('.')
        current = data
        
        for key in keys[:-1]:
            if key not in current:
                current[key] = {}
            current = current[key]
            
        current[keys[-1]] = value
        
    def _check_field_condition(self, value: Any, condition: Dict[str, Any]) -> bool:
        """Check field condition"""
        operator = condition.get('operator', 'eq')
        expected = condition.get('value')
        
        if operator == 'eq':
            return value == expected
        elif operator == 'ne':
            return value != expected
        elif operator == 'gt':
            return value > expected
        elif operator == 'lt':
            return value < expected
        elif operator == 'gte':
            return value >= expected
        elif operator == 'lte':
            return value <= expected
        elif operator == 'in':
            return value in expected
        elif operator == 'contains':
            return expected in str(value) if value else False
        elif operator == 'regex':
            import re
            return bool(re.match(expected, str(value))) if value else False
            
        return False


class StreamingDataEnricher:
    """
    Streaming data enricher with external API integration
    """
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.enrichment_rules: Dict[str, EnrichmentRule] = {}
        self.enrichment_service = EnrichmentService()
        self.enrichment_cache: Dict[str, Any] = {}
        self.cache_timestamps: Dict[str, datetime] = {}
        self.metrics_collector = get_metrics_collector()
        
    def add_enrichment_rule(self, rule: EnrichmentRule):
        """Add enrichment rule"""
        self.enrichment_rules[rule.rule_id] = rule
        self.logger.info(f"Added enrichment rule: {rule.name}")
        
    async def enrich_data(self, data: Dict[str, Any]) -> Dict[str, Any]:
        """Enrich data using configured rules"""
        enriched_data = data.copy()
        enrichments = {}
        
        # Apply enrichment rules
        for rule in self.enrichment_rules.values():
            if not rule.enabled:
                continue
                
            try:
                enrichment = await self._apply_enrichment_rule(rule, enriched_data)
                if enrichment:
                    enrichments[rule.rule_id] = enrichment
                    enriched_data.update(enrichment)
            except Exception as e:
                self.logger.error(f"Enrichment rule {rule.rule_id} failed: {e}")
                
        # Add enrichment metadata
        enriched_data['_enrichments'] = enrichments
        enriched_data['_enrichment_timestamp'] = datetime.now().isoformat()
        
        return enriched_data
        
    async def _apply_enrichment_rule(self, rule: EnrichmentRule, data: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Apply specific enrichment rule"""
        lookup_value = self._get_nested_value(data, rule.lookup_field)
        if not lookup_value:
            return None
            
        # Check cache first
        cache_key = f"{rule.source.value}:{lookup_value}"
        cached_data = self._get_from_cache(cache_key, rule.cache_ttl_seconds)
        
        if cached_data:
            return cached_data
            
        # Fetch from external source
        try:
            if rule.source == EnrichmentSource.CUSTOMER_API:
                enrichment_data = await self._enrich_from_customer_api(lookup_value, rule)
            elif rule.source == EnrichmentSource.PRODUCT_API:
                enrichment_data = await self._enrich_from_product_api(lookup_value, rule)
            elif rule.source == EnrichmentSource.CURRENCY_API:
                enrichment_data = await self._enrich_from_currency_api(lookup_value, rule)
            elif rule.source == EnrichmentSource.LOCATION_API:
                enrichment_data = await self._enrich_from_location_api(lookup_value, rule)
            else:
                enrichment_data = None
                
            # Cache result
            if enrichment_data:
                self._store_in_cache(cache_key, enrichment_data)
                return enrichment_data
            else:
                return self._get_fallback_data(rule)
                
        except Exception as e:
            self.logger.error(f"External enrichment failed for {rule.source.value}: {e}")
            return self._get_fallback_data(rule)
            
    async def _enrich_from_customer_api(self, customer_id: str, rule: EnrichmentRule) -> Optional[Dict[str, Any]]:
        """Enrich from customer API"""
        try:
            customer_data = await self.enrichment_service.get_customer_details(customer_id)
            if customer_data:
                # Map to target fields
                enrichment = {}
                for target_field in rule.target_fields:
                    if target_field in customer_data:
                        enrichment[target_field] = customer_data[target_field]
                return enrichment
        except Exception as e:
            self.logger.error(f"Customer API enrichment failed: {e}")
        return None
        
    async def _enrich_from_product_api(self, product_code: str, rule: EnrichmentRule) -> Optional[Dict[str, Any]]:
        """Enrich from product API"""
        try:
            product_data = await self.enrichment_service.get_product_details(product_code)
            if product_data:
                enrichment = {}
                for target_field in rule.target_fields:
                    if target_field in product_data:
                        enrichment[target_field] = product_data[target_field]
                return enrichment
        except Exception as e:
            self.logger.error(f"Product API enrichment failed: {e}")
        return None
        
    async def _enrich_from_currency_api(self, currency_code: str, rule: EnrichmentRule) -> Optional[Dict[str, Any]]:
        """Enrich from currency API"""
        try:
            currency_data = await self.enrichment_service.get_exchange_rate(currency_code)
            if currency_data:
                enrichment = {}
                for target_field in rule.target_fields:
                    if target_field in currency_data:
                        enrichment[target_field] = currency_data[target_field]
                return enrichment
        except Exception as e:
            self.logger.error(f"Currency API enrichment failed: {e}")
        return None
        
    async def _enrich_from_location_api(self, location: str, rule: EnrichmentRule) -> Optional[Dict[str, Any]]:
        """Enrich from location API"""
        try:
            location_data = await self.enrichment_service.get_location_details(location)
            if location_data:
                enrichment = {}
                for target_field in rule.target_fields:
                    if target_field in location_data:
                        enrichment[target_field] = location_data[target_field]
                return enrichment
        except Exception as e:
            self.logger.error(f"Location API enrichment failed: {e}")
        return None
        
    def _get_from_cache(self, cache_key: str, ttl_seconds: int) -> Optional[Dict[str, Any]]:
        """Get data from cache if not expired"""
        if cache_key not in self.enrichment_cache:
            return None
            
        # Check if expired
        cached_time = self.cache_timestamps.get(cache_key)
        if cached_time and (datetime.now() - cached_time).total_seconds() > ttl_seconds:
            # Remove expired entry
            del self.enrichment_cache[cache_key]
            del self.cache_timestamps[cache_key]
            return None
            
        return self.enrichment_cache[cache_key]
        
    def _store_in_cache(self, cache_key: str, data: Dict[str, Any]):
        """Store data in cache"""
        self.enrichment_cache[cache_key] = data
        self.cache_timestamps[cache_key] = datetime.now()
        
        # Limit cache size
        if len(self.enrichment_cache) > 10000:
            # Remove oldest entries
            oldest_keys = sorted(
                self.cache_timestamps.items(),
                key=lambda x: x[1]
            )[:1000]
            
            for key, _ in oldest_keys:
                if key in self.enrichment_cache:
                    del self.enrichment_cache[key]
                if key in self.cache_timestamps:
                    del self.cache_timestamps[key]
                    
    def _get_fallback_data(self, rule: EnrichmentRule) -> Optional[Dict[str, Any]]:
        """Get fallback data for failed enrichment"""
        if rule.fallback_value:
            return {field: rule.fallback_value for field in rule.target_fields}
        return None
        
    def _get_nested_value(self, data: Dict[str, Any], field_path: str) -> Any:
        """Get nested field value using dot notation"""
        keys = field_path.split('.')
        value = data
        
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return None
                
        return value


class RealtimeTransformationPipeline:
    """
    Complete real-time transformation and enrichment pipeline
    """
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        self.config = config or {}
        self.logger = get_logger(__name__)
        self.kafka_manager = KafkaManager()
        self.transformer = StreamingDataTransformer()
        self.enricher = StreamingDataEnricher()
        self.metrics_collector = get_metrics_collector()
        
        # Processing state
        self.running = False
        self.processing_threads: List[threading.Thread] = []
        self.processed_count = 0
        self.failed_count = 0
        self.start_time = None
        
    def initialize(self):
        """Initialize transformation pipeline"""
        self.logger.info("Initializing Realtime Transformation Pipeline")
        
        # Load default transformation rules
        self._load_default_transformation_rules()
        
        # Load default enrichment rules
        self._load_default_enrichment_rules()
        
        self.logger.info("Realtime Transformation Pipeline initialized")
        
    def _load_default_transformation_rules(self):
        """Load default transformation rules"""
        
        # Data cleansing rule
        cleansing_rule = TransformationRule(
            rule_id="data_cleansing",
            name="General Data Cleansing",
            transformation_type=TransformationType.CLEANSING,
            source_fields=["customer_id", "invoice_no", "description"],
            target_fields=["customer_id", "invoice_no", "description"],
            conditions={},
            transformation_logic="remove_special_chars,proper_case",
            priority=1
        )
        self.transformer.add_transformation_rule(cleansing_rule)
        
        # Data validation rule
        validation_rule = TransformationRule(
            rule_id="data_validation",
            name="Data Validation",
            transformation_type=TransformationType.VALIDATION,
            source_fields=["invoice_no", "customer_id", "quantity", "unit_price"],
            target_fields=[],
            conditions={},
            transformation_logic="required",
            priority=2
        )
        self.transformer.add_transformation_rule(validation_rule)
        
        # Date formatting rule
        date_formatting_rule = TransformationRule(
            rule_id="date_formatting",
            name="Date Formatting",
            transformation_type=TransformationType.FORMATTING,
            source_fields=["invoice_date"],
            target_fields=["invoice_date_formatted"],
            conditions={},
            transformation_logic=json.dumps({"date_format": "%Y-%m-%d"}),
            priority=3
        )
        self.transformer.add_transformation_rule(date_formatting_rule)
        
    def _load_default_enrichment_rules(self):
        """Load default enrichment rules"""
        
        # Customer enrichment
        customer_enrichment = EnrichmentRule(
            rule_id="customer_enrichment",
            name="Customer Data Enrichment",
            source=EnrichmentSource.CUSTOMER_API,
            lookup_field="customer_id",
            target_fields=["customer_name", "customer_segment", "customer_location"],
            cache_ttl_seconds=3600
        )
        self.enricher.add_enrichment_rule(customer_enrichment)
        
        # Product enrichment
        product_enrichment = EnrichmentRule(
            rule_id="product_enrichment",
            name="Product Data Enrichment",
            source=EnrichmentSource.PRODUCT_API,
            lookup_field="stock_code",
            target_fields=["product_name", "product_category", "product_brand"],
            cache_ttl_seconds=7200
        )
        self.enricher.add_enrichment_rule(product_enrichment)
        
        # Currency enrichment
        currency_enrichment = EnrichmentRule(
            rule_id="currency_enrichment",
            name="Currency Data Enrichment",
            source=EnrichmentSource.CURRENCY_API,
            lookup_field="country",
            target_fields=["currency_code", "exchange_rate"],
            cache_ttl_seconds=1800
        )
        self.enricher.add_enrichment_rule(currency_enrichment)
        
    def start_pipeline(self, source_topics: List[str], output_topic: str, 
                      consumer_group: str = "realtime_transformation"):
        """Start transformation pipeline"""
        if self.running:
            self.logger.warning("Pipeline already running")
            return
            
        self.running = True
        self.start_time = datetime.now()
        self.logger.info(f"Starting transformation pipeline for topics: {source_topics}")
        
        # Start processing threads
        for topic in source_topics:
            thread = threading.Thread(
                target=self._process_topic_pipeline,
                args=(topic, output_topic, consumer_group),
                name=f"TransformPipeline-{topic}"
            )
            thread.daemon = True
            thread.start()
            self.processing_threads.append(thread)
            
        self.logger.info(f"Started {len(self.processing_threads)} transformation threads")
        
    def stop_pipeline(self):
        """Stop transformation pipeline"""
        if not self.running:
            return
            
        self.logger.info("Stopping transformation pipeline")
        self.running = False
        
        # Wait for threads
        for thread in self.processing_threads:
            thread.join(timeout=30)
            
        self.processing_threads.clear()
        self.logger.info("Transformation pipeline stopped")
        
    def _process_topic_pipeline(self, source_topic: str, output_topic: str, consumer_group: str):
        """Process transformation pipeline for specific topic"""
        try:
            consumer = self.kafka_manager.create_consumer([source_topic], f"{consumer_group}_{source_topic}")
            
            self.logger.info(f"Started transformation pipeline for topic: {source_topic}")
            
            while self.running:
                try:
                    # Poll for messages
                    message_batch = consumer.poll(timeout_ms=1000)
                    
                    if not message_batch:
                        continue
                        
                    # Process messages
                    for topic_partition, messages in message_batch.items():
                        for message in messages:
                            try:
                                result = asyncio.run(self._transform_message(message))
                                
                                if result and result.success:
                                    # Send transformed data to output topic
                                    self._send_transformed_data(result, output_topic)
                                    self.processed_count += 1
                                else:
                                    self.failed_count += 1
                                    
                            except Exception as e:
                                self.logger.error(f"Message transformation failed: {e}")
                                self.failed_count += 1
                                
                    # Commit offsets
                    consumer.commit()
                    
                except Exception as e:
                    self.logger.error(f"Batch processing error for topic {source_topic}: {e}")
                    time.sleep(5)
                    
        except Exception as e:
            self.logger.error(f"Fatal error in transformation pipeline {source_topic}: {e}")
        finally:
            try:
                consumer.close()
            except:
                pass
                
    async def _transform_message(self, message) -> Optional[TransformationResult]:
        """Transform single message"""
        start_time = time.time()
        
        try:
            # Extract message data
            message_data = message.value
            
            # Apply transformations
            transformed_data = self.transformer.transform(message_data)
            
            # Apply enrichments
            enriched_data = await self.enricher.enrich_data(transformed_data)
            
            # Validate transformed data
            validation_errors = self.transformer.validate(enriched_data)
            
            # Check if data should be filtered
            filtered = enriched_data.get('_filtered', False)
            if filtered:
                return None
                
            processing_time = (time.time() - start_time) * 1000
            
            return TransformationResult(
                success=len(validation_errors) == 0,
                original_data=message_data,
                transformed_data=enriched_data,
                applied_rules=list(self.transformer.transformation_rules.keys()),
                enrichments=enriched_data.get('_enrichments', {}),
                validation_errors=validation_errors,
                processing_time_ms=processing_time
            )
            
        except Exception as e:
            self.logger.error(f"Message transformation failed: {e}")
            return TransformationResult(
                success=False,
                original_data=message.value,
                transformed_data={},
                applied_rules=[],
                enrichments={},
                validation_errors=[str(e)],
                processing_time_ms=(time.time() - start_time) * 1000
            )
            
    def _send_transformed_data(self, result: TransformationResult, output_topic: str):
        """Send transformed data to output topic"""
        try:
            # Create message with transformation metadata
            output_message = {
                "original_data": result.original_data,
                "transformed_data": result.transformed_data,
                "transformation_metadata": {
                    "applied_rules": result.applied_rules,
                    "enrichments": result.enrichments,
                    "validation_errors": result.validation_errors,
                    "processing_time_ms": result.processing_time_ms,
                    "pipeline_timestamp": result.timestamp.isoformat()
                }
            }
            
            # Send to output topic
            self.kafka_manager.produce_message(
                topic=output_topic,
                message=output_message,
                key=result.transformed_data.get('invoice_no', 'unknown')
            )
            
            # Record metrics
            if self.metrics_collector:
                self.metrics_collector.increment_counter("transformation_processed", {
                    "success": str(result.success),
                    "output_topic": output_topic
                })
                
                self.metrics_collector.record_histogram("transformation_processing_time_ms", 
                                                       result.processing_time_ms)
                
        except Exception as e:
            self.logger.error(f"Failed to send transformed data: {e}")
            
    def get_pipeline_stats(self) -> Dict[str, Any]:
        """Get pipeline statistics"""
        uptime = 0
        if self.start_time:
            uptime = (datetime.now() - self.start_time).total_seconds()
            
        return {
            "running": self.running,
            "uptime_seconds": uptime,
            "processed_count": self.processed_count,
            "failed_count": self.failed_count,
            "success_rate": self.processed_count / max(self.processed_count + self.failed_count, 1),
            "processing_rate": self.processed_count / max(uptime, 1),
            "active_threads": len(self.processing_threads),
            "transformation_rules": len(self.transformer.transformation_rules),
            "enrichment_rules": len(self.enricher.enrichment_rules),
            "cache_size": len(self.enricher.enrichment_cache),
            "timestamp": datetime.now().isoformat()
        }


# Factory function
def create_realtime_transformation_pipeline(config: Optional[Dict[str, Any]] = None) -> RealtimeTransformationPipeline:
    """Create realtime transformation pipeline"""
    return RealtimeTransformationPipeline(config)


# Example usage
if __name__ == "__main__":
    pipeline = create_realtime_transformation_pipeline()
    pipeline.initialize()
    
    source_topics = [StreamingTopic.RETAIL_TRANSACTIONS.value]
    output_topic = "transformed_retail_transactions"
    
    pipeline.start_pipeline(source_topics, output_topic)
    
    print("Realtime Transformation Pipeline started. Press Ctrl+C to stop.")
    
    try:
        while pipeline.running:
            time.sleep(30)
            stats = pipeline.get_pipeline_stats()
            print(f"Pipeline Stats: {stats['processed_count']} processed, "
                  f"{stats['success_rate']:.2%} success rate, "
                  f"{stats['processing_rate']:.2f} records/sec")
    except KeyboardInterrupt:
        pipeline.stop_pipeline()