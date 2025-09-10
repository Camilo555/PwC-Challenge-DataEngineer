"""
Real-Time Dashboard ETL Processor
Specialized streaming processor for Business Intelligence Dashboard KPIs
"""
import asyncio
import json
import hashlib
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set, Union, Callable
from dataclasses import dataclass, field
from enum import Enum
import logging

from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
from confluent_kafka.avro import AvroProducer, AvroConsumer
import redis.asyncio as aioredis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from src.streaming.kafka_manager import StreamingTopic, create_kafka_manager
from src.core.database.async_db_manager import get_async_db_session
from src.api.dashboard.dashboard_cache_manager import create_dashboard_cache_manager
from src.etl.gold.materialized_views_manager import create_materialized_views_manager
from core.config.unified_config import get_unified_config
from core.logging import get_logger


class DashboardEventType(Enum):
    """Dashboard-specific event types"""
    KPI_UPDATE = "kpi_update"
    ANOMALY_DETECTED = "anomaly_detected"
    THRESHOLD_BREACH = "threshold_breach"
    DATA_REFRESH = "data_refresh"
    USER_INTERACTION = "user_interaction"
    CACHE_INVALIDATION = "cache_invalidation"
    ALERT_TRIGGERED = "alert_triggered"
    PERFORMANCE_METRIC = "performance_metric"


class DashboardDataSource(Enum):
    """Dashboard data source types"""
    SALES_TRANSACTIONS = "sales_transactions"
    CUSTOMER_INTERACTIONS = "customer_interactions"
    PRODUCT_CATALOG = "product_catalog"
    OPERATIONAL_METRICS = "operational_metrics"
    FINANCIAL_DATA = "financial_data"
    USER_BEHAVIOR = "user_behavior"


@dataclass
class DashboardKPIEvent:
    """Real-time KPI event structure"""
    event_id: str
    event_type: DashboardEventType
    kpi_name: str
    current_value: float
    previous_value: Optional[float]
    change_percentage: Optional[float]
    timestamp: datetime
    data_source: DashboardDataSource
    business_unit: str
    user_id: Optional[str] = None
    alert_threshold: Optional[float] = None
    is_anomaly: bool = False
    confidence_score: float = 1.0
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            "event_id": self.event_id,
            "event_type": self.event_type.value,
            "kpi_name": self.kpi_name,
            "current_value": self.current_value,
            "previous_value": self.previous_value,
            "change_percentage": self.change_percentage,
            "timestamp": self.timestamp.isoformat(),
            "data_source": self.data_source.value,
            "business_unit": self.business_unit,
            "user_id": self.user_id,
            "alert_threshold": self.alert_threshold,
            "is_anomaly": self.is_anomaly,
            "confidence_score": self.confidence_score,
            "metadata": self.metadata
        }


@dataclass
class DashboardProcessorConfig:
    """Configuration for dashboard processor"""
    kafka_bootstrap_servers: str = "localhost:9092"
    consumer_group: str = "dashboard-processor"
    batch_size: int = 1000
    commit_interval: int = 5000  # milliseconds
    enable_auto_commit: bool = False
    session_timeout: int = 30000
    max_poll_records: int = 500
    processing_timeout: int = 30  # seconds
    enable_compression: bool = True
    redis_url: str = "redis://localhost:6379"
    enable_materialized_view_refresh: bool = True
    enable_cache_warming: bool = True


class RealTimeDashboardProcessor:
    """
    Real-time stream processor for Business Intelligence Dashboard KPIs
    
    Processes streaming events to update dashboard KPIs in real-time with:
    - Statistical anomaly detection
    - Intelligent cache invalidation
    - Materialized view refresh optimization
    - WebSocket notification publishing
    """
    
    def __init__(self, config: Optional[DashboardProcessorConfig] = None):
        self.config = config or DashboardProcessorConfig()
        self.logger = get_logger(__name__)
        self.unified_config = get_unified_config()
        
        # Initialize components
        self.kafka_manager = create_kafka_manager()
        self.dashboard_cache = create_dashboard_cache_manager()
        self.views_manager = create_materialized_views_manager()
        
        # Processing state
        self.is_running = False
        self.processed_events = 0
        self.failed_events = 0
        self.last_processed_timestamp = datetime.now()
        
        # Event processors mapping
        self.event_processors = {
            DashboardEventType.KPI_UPDATE: self._process_kpi_update,
            DashboardEventType.ANOMALY_DETECTED: self._process_anomaly_detection,
            DashboardEventType.THRESHOLD_BREACH: self._process_threshold_breach,
            DashboardEventType.DATA_REFRESH: self._process_data_refresh,
            DashboardEventType.USER_INTERACTION: self._process_user_interaction,
            DashboardEventType.CACHE_INVALIDATION: self._process_cache_invalidation,
            DashboardEventType.ALERT_TRIGGERED: self._process_alert_triggered,
            DashboardEventType.PERFORMANCE_METRIC: self._process_performance_metric
        }
        
        # KPI calculation cache
        self.kpi_cache: Dict[str, Dict[str, Any]] = {}
        
        # WebSocket connection registry
        self.websocket_connections: Dict[str, Set[Any]] = {}
        
        # Statistical analysis window
        self.analysis_window_hours = 24
        self.anomaly_threshold = 2.0  # Z-score threshold
        
    async def start_processing(self):
        """Start the real-time dashboard processing"""
        if self.is_running:
            self.logger.warning("Dashboard processor is already running")
            return
            
        self.is_running = True
        self.logger.info("Starting Real-time Dashboard Processor")
        
        # Start processing tasks
        tasks = [
            asyncio.create_task(self._consume_dashboard_events()),
            asyncio.create_task(self._periodic_kpi_refresh()),
            asyncio.create_task(self._periodic_cache_warmup()),
            asyncio.create_task(self._publish_processing_metrics())
        ]
        
        try:
            await asyncio.gather(*tasks)
        except Exception as e:
            self.logger.error(f"Dashboard processor error: {e}")
            self.is_running = False
            raise
    
    async def stop_processing(self):
        """Stop the dashboard processor"""
        self.is_running = False
        self.logger.info("Stopping Real-time Dashboard Processor")
        
        # Cleanup resources
        await self.dashboard_cache.close()
        await self.views_manager.close()
        self.kafka_manager.close()
    
    async def _consume_dashboard_events(self):
        """Consume and process dashboard events from Kafka"""
        # Subscribe to relevant topics
        topics = [
            StreamingTopic.RETAIL_TRANSACTIONS.value,
            StreamingTopic.CUSTOMER_EVENTS.value,
            StreamingTopic.SYSTEM_EVENTS.value,
            StreamingTopic.METRICS.value,
            "dashboard-kpi-updates",  # Custom dashboard topic
            "dashboard-alerts",
            "dashboard-user-interactions"
        ]
        
        consumer_config = {
            'bootstrap.servers': self.config.kafka_bootstrap_servers,
            'group.id': self.config.consumer_group,
            'auto.offset.reset': 'latest',
            'enable.auto.commit': self.config.enable_auto_commit,
            'session.timeout.ms': self.config.session_timeout,
            'max.poll.interval.ms': self.config.processing_timeout * 1000,
            'fetch.min.bytes': 1,
            'fetch.wait.max.ms': 500
        }
        
        consumer = Consumer(consumer_config)
        consumer.subscribe(topics)
        
        self.logger.info(f"Subscribed to topics: {topics}")
        
        try:
            while self.is_running:
                msg = consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                    
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        self.logger.error(f"Consumer error: {msg.error()}")
                        break
                
                # Process the message
                await self._process_kafka_message(msg)
                
                # Commit offset
                if not self.config.enable_auto_commit:
                    consumer.commit(asynchronous=False)
                    
        except KeyboardInterrupt:
            self.logger.info("Consumer interrupted by user")
        except Exception as e:
            self.logger.error(f"Consumer error: {e}")
        finally:
            consumer.close()
    
    async def _process_kafka_message(self, msg):
        """Process individual Kafka message"""
        try:
            # Decode message
            topic = msg.topic()
            key = msg.key().decode('utf-8') if msg.key() else None
            value = json.loads(msg.value().decode('utf-8'))
            
            # Create dashboard event
            event = await self._create_dashboard_event(topic, key, value)
            if not event:
                return
                
            # Process based on event type
            processor = self.event_processors.get(event.event_type)
            if processor:
                await processor(event)
                self.processed_events += 1
                self.last_processed_timestamp = datetime.now()
            else:
                self.logger.warning(f"No processor found for event type: {event.event_type}")
                
        except Exception as e:
            self.failed_events += 1
            self.logger.error(f"Error processing message: {e}")
    
    async def _create_dashboard_event(self, topic: str, key: Optional[str], value: Dict[str, Any]) -> Optional[DashboardKPIEvent]:
        """Create dashboard event from Kafka message"""
        try:
            # Map topic to event type and data source
            topic_mapping = {
                "retail-transactions": (DashboardEventType.KPI_UPDATE, DashboardDataSource.SALES_TRANSACTIONS),
                "customer-events": (DashboardEventType.KPI_UPDATE, DashboardDataSource.CUSTOMER_INTERACTIONS),
                "system-events": (DashboardEventType.PERFORMANCE_METRIC, DashboardDataSource.OPERATIONAL_METRICS),
                "metrics": (DashboardEventType.PERFORMANCE_METRIC, DashboardDataSource.OPERATIONAL_METRICS),
                "dashboard-kpi-updates": (DashboardEventType.KPI_UPDATE, DashboardDataSource.SALES_TRANSACTIONS),
                "dashboard-alerts": (DashboardEventType.ALERT_TRIGGERED, DashboardDataSource.OPERATIONAL_METRICS),
                "dashboard-user-interactions": (DashboardEventType.USER_INTERACTION, DashboardDataSource.USER_BEHAVIOR)
            }
            
            event_type, data_source = topic_mapping.get(topic, (DashboardEventType.KPI_UPDATE, DashboardDataSource.SALES_TRANSACTIONS))
            
            # Extract KPI information
            kpi_name = self._extract_kpi_name(topic, value)
            current_value = self._extract_kpi_value(topic, value)
            
            if kpi_name is None or current_value is None:
                return None
            
            # Get previous value for trend analysis
            previous_value = await self._get_previous_kpi_value(kpi_name)
            change_percentage = None
            if previous_value and previous_value != 0:
                change_percentage = ((current_value - previous_value) / previous_value) * 100
            
            # Anomaly detection
            is_anomaly = await self._detect_anomaly(kpi_name, current_value)
            confidence_score = await self._calculate_confidence_score(kpi_name, current_value)
            
            return DashboardKPIEvent(
                event_id=key or str(uuid.uuid4()),
                event_type=event_type,
                kpi_name=kpi_name,
                current_value=current_value,
                previous_value=previous_value,
                change_percentage=change_percentage,
                timestamp=datetime.fromisoformat(value.get('timestamp', datetime.now().isoformat())),
                data_source=data_source,
                business_unit=value.get('business_unit', 'default'),
                user_id=value.get('user_id'),
                is_anomaly=is_anomaly,
                confidence_score=confidence_score,
                metadata=value.get('metadata', {})
            )
            
        except Exception as e:
            self.logger.error(f"Error creating dashboard event: {e}")
            return None
    
    def _extract_kpi_name(self, topic: str, value: Dict[str, Any]) -> Optional[str]:
        """Extract KPI name from message"""
        # Topic-specific KPI extraction logic
        if topic == "retail-transactions":
            return "hourly_revenue"  # Default KPI for sales
        elif topic == "customer-events":
            return "active_customers"
        elif topic == "system-events":
            return "system_performance"
        elif topic == "dashboard-kpi-updates":
            return value.get('kpi_name')
        else:
            return value.get('metric_name', 'unknown_kpi')
    
    def _extract_kpi_value(self, topic: str, value: Dict[str, Any]) -> Optional[float]:
        """Extract KPI value from message"""
        # Topic-specific value extraction logic
        try:
            if topic == "retail-transactions":
                return float(value.get('total_amount', 0))
            elif topic == "customer-events":
                return float(value.get('customer_count', 1))
            elif topic == "system-events":
                return float(value.get('performance_score', 0))
            elif topic == "dashboard-kpi-updates":
                return float(value.get('value', 0))
            else:
                return float(value.get('value', 0))
        except (ValueError, TypeError):
            return None
    
    async def _get_previous_kpi_value(self, kpi_name: str) -> Optional[float]:
        """Get previous KPI value for trend analysis"""
        try:
            cached_value = self.kpi_cache.get(kpi_name, {}).get('value')
            return cached_value
        except Exception as e:
            self.logger.error(f"Error getting previous KPI value: {e}")
            return None
    
    async def _detect_anomaly(self, kpi_name: str, current_value: float) -> bool:
        """Detect if current value is anomalous using statistical analysis"""
        try:
            # Get historical data from materialized views
            async with get_async_db_session() as session:
                query = text("""
                    SELECT value FROM kpi_history 
                    WHERE kpi_name = :kpi_name 
                    AND timestamp >= :start_time 
                    ORDER BY timestamp DESC 
                    LIMIT 100
                """)
                
                start_time = datetime.now() - timedelta(hours=self.analysis_window_hours)
                result = await session.execute(query, {
                    "kpi_name": kpi_name,
                    "start_time": start_time
                })
                
                historical_values = [row.value for row in result.fetchall()]
                
                if len(historical_values) < 10:  # Need minimum data for analysis
                    return False
                
                # Calculate z-score
                import numpy as np
                mean = np.mean(historical_values)
                std = np.std(historical_values)
                
                if std == 0:
                    return False
                    
                z_score = abs((current_value - mean) / std)
                return z_score > self.anomaly_threshold
                
        except Exception as e:
            self.logger.error(f"Error in anomaly detection: {e}")
            return False
    
    async def _calculate_confidence_score(self, kpi_name: str, current_value: float) -> float:
        """Calculate confidence score for the KPI value"""
        try:
            # Simple confidence calculation based on data freshness and completeness
            base_confidence = 1.0
            
            # Check data freshness
            last_update = self.kpi_cache.get(kpi_name, {}).get('timestamp')
            if last_update:
                age_minutes = (datetime.now() - last_update).total_seconds() / 60
                if age_minutes > 60:  # Older than 1 hour
                    base_confidence *= 0.8
            
            return max(0.0, min(1.0, base_confidence))
            
        except Exception as e:
            self.logger.error(f"Error calculating confidence score: {e}")
            return 0.5
    
    async def _process_kpi_update(self, event: DashboardKPIEvent):
        """Process KPI update event"""
        try:
            # Update KPI cache
            self.kpi_cache[event.kpi_name] = {
                'value': event.current_value,
                'timestamp': event.timestamp,
                'change_percentage': event.change_percentage,
                'is_anomaly': event.is_anomaly
            }
            
            # Invalidate related dashboard cache entries
            await self.dashboard_cache.invalidate_cache(
                cache_name="executive_kpis",
                pattern=f"*{event.kpi_name}*"
            )
            
            # Refresh materialized views if needed
            if self.config.enable_materialized_view_refresh:
                await self._refresh_relevant_views(event.kpi_name)
            
            # Publish to WebSocket clients
            await self._publish_websocket_update(event)
            
            # Log significant changes
            if event.is_anomaly or (event.change_percentage and abs(event.change_percentage) > 10):
                self.logger.info(f"Significant KPI change: {event.kpi_name} = {event.current_value} ({event.change_percentage:+.1f}%)")
            
        except Exception as e:
            self.logger.error(f"Error processing KPI update: {e}")
    
    async def _process_anomaly_detection(self, event: DashboardKPIEvent):
        """Process anomaly detection event"""
        try:
            self.logger.warning(f"Anomaly detected: {event.kpi_name} = {event.current_value} (confidence: {event.confidence_score})")
            
            # Store anomaly in database for analysis
            await self._store_anomaly_record(event)
            
            # Trigger alert if confidence is high
            if event.confidence_score > 0.8:
                await self._trigger_anomaly_alert(event)
            
            # Update dashboard with anomaly flag
            await self._publish_websocket_update(event, include_anomaly=True)
            
        except Exception as e:
            self.logger.error(f"Error processing anomaly detection: {e}")
    
    async def _process_threshold_breach(self, event: DashboardKPIEvent):
        """Process threshold breach event"""
        try:
            self.logger.warning(f"Threshold breach: {event.kpi_name} = {event.current_value}, threshold = {event.alert_threshold}")
            
            # Immediate alert for threshold breaches
            await self._trigger_threshold_alert(event)
            
            # High-priority WebSocket notification
            await self._publish_websocket_update(event, priority="high")
            
        except Exception as e:
            self.logger.error(f"Error processing threshold breach: {e}")
    
    async def _process_data_refresh(self, event: DashboardKPIEvent):
        """Process data refresh event"""
        try:
            # Refresh materialized views
            await self._refresh_relevant_views(event.kpi_name)
            
            # Warm cache for related data
            if self.config.enable_cache_warming:
                await self.dashboard_cache.warm_up_cache([event.business_unit])
            
        except Exception as e:
            self.logger.error(f"Error processing data refresh: {e}")
    
    async def _process_user_interaction(self, event: DashboardKPIEvent):
        """Process user interaction event"""
        try:
            # Track user behavior for personalization
            user_id = event.user_id
            if user_id:
                # Update user preferences and cache accordingly
                await self._update_user_preferences(user_id, event)
            
        except Exception as e:
            self.logger.error(f"Error processing user interaction: {e}")
    
    async def _process_cache_invalidation(self, event: DashboardKPIEvent):
        """Process cache invalidation event"""
        try:
            await self.dashboard_cache.invalidate_cache(
                cache_name=event.metadata.get('cache_name'),
                pattern=event.metadata.get('pattern')
            )
            
        except Exception as e:
            self.logger.error(f"Error processing cache invalidation: {e}")
    
    async def _process_alert_triggered(self, event: DashboardKPIEvent):
        """Process alert triggered event"""
        try:
            # Send alert through multiple channels
            await self._send_multi_channel_alert(event)
            
            # Update alert status in dashboard
            await self._publish_websocket_update(event, alert_type="triggered")
            
        except Exception as e:
            self.logger.error(f"Error processing alert: {e}")
    
    async def _process_performance_metric(self, event: DashboardKPIEvent):
        """Process performance metric event"""
        try:
            # Track system performance metrics
            await self._store_performance_metric(event)
            
            # Check for performance degradation
            if event.current_value < 0.95:  # Performance below 95%
                await self._trigger_performance_alert(event)
            
        except Exception as e:
            self.logger.error(f"Error processing performance metric: {e}")
    
    async def _refresh_relevant_views(self, kpi_name: str):
        """Refresh materialized views relevant to the KPI"""
        try:
            # Map KPIs to relevant materialized views
            view_mappings = {
                "hourly_revenue": ["mv_executive_kpis", "mv_revenue_analytics"],
                "active_customers": ["mv_customer_behavior", "mv_executive_kpis"],
                "system_performance": ["mv_operational_metrics"],
                "data_quality_score": ["mv_data_quality_dashboard"]
            }
            
            views_to_refresh = view_mappings.get(kpi_name, [])
            for view_name in views_to_refresh:
                await self.views_manager.refresh_view(view_name, concurrent=True)
                
        except Exception as e:
            self.logger.error(f"Error refreshing materialized views: {e}")
    
    async def _publish_websocket_update(self, event: DashboardKPIEvent, **kwargs):
        """Publish update to WebSocket clients"""
        try:
            message = {
                "type": "kpi_update",
                "data": event.to_dict(),
                "timestamp": datetime.now().isoformat(),
                **kwargs
            }
            
            # Send to all connected clients (simplified - in reality would be more targeted)
            for user_id, connections in self.websocket_connections.items():
                for websocket in connections:
                    try:
                        await websocket.send_json(message)
                    except Exception as e:
                        self.logger.error(f"Error sending WebSocket message to {user_id}: {e}")
                        
        except Exception as e:
            self.logger.error(f"Error publishing WebSocket update: {e}")
    
    async def _periodic_kpi_refresh(self):
        """Periodic KPI refresh task"""
        while self.is_running:
            try:
                # Refresh key KPIs every minute
                await asyncio.sleep(60)
                
                # Calculate current KPIs
                await self._calculate_real_time_kpis()
                
            except Exception as e:
                self.logger.error(f"Error in periodic KPI refresh: {e}")
    
    async def _periodic_cache_warmup(self):
        """Periodic cache warmup task"""
        while self.is_running:
            try:
                # Warm up cache every 5 minutes
                await asyncio.sleep(300)
                
                if self.config.enable_cache_warming:
                    await self.dashboard_cache.warm_up_cache()
                
            except Exception as e:
                self.logger.error(f"Error in periodic cache warmup: {e}")
    
    async def _publish_processing_metrics(self):
        """Publish processing metrics"""
        while self.is_running:
            try:
                await asyncio.sleep(60)  # Every minute
                
                metrics = {
                    "processed_events": self.processed_events,
                    "failed_events": self.failed_events,
                    "success_rate": self.processed_events / (self.processed_events + self.failed_events) if (self.processed_events + self.failed_events) > 0 else 0,
                    "last_processed": self.last_processed_timestamp.isoformat(),
                    "active_connections": sum(len(connections) for connections in self.websocket_connections.values()),
                    "cached_kpis": len(self.kpi_cache)
                }
                
                # Publish to monitoring topic
                await self.kafka_manager.produce_message(
                    topic=StreamingTopic.METRICS,
                    message=metrics,
                    key="dashboard_processor_metrics"
                )
                
            except Exception as e:
                self.logger.error(f"Error publishing processing metrics: {e}")
    
    async def _calculate_real_time_kpis(self):
        """Calculate real-time KPIs from current data"""
        try:
            async with get_async_db_session() as session:
                # Calculate hourly revenue
                revenue_query = text("""
                    SELECT COALESCE(SUM(total_amount), 0) as hourly_revenue
                    FROM silver_sales_clean 
                    WHERE created_at >= date_trunc('hour', NOW())
                """)
                
                result = await session.execute(revenue_query)
                hourly_revenue = result.scalar()
                
                # Create and process KPI event
                if hourly_revenue is not None:
                    event = DashboardKPIEvent(
                        event_id=f"kpi_refresh_{datetime.now().strftime('%Y%m%d_%H%M%S')}",
                        event_type=DashboardEventType.KPI_UPDATE,
                        kpi_name="hourly_revenue",
                        current_value=float(hourly_revenue),
                        previous_value=self.kpi_cache.get("hourly_revenue", {}).get('value'),
                        change_percentage=None,
                        timestamp=datetime.now(),
                        data_source=DashboardDataSource.SALES_TRANSACTIONS,
                        business_unit="sales"
                    )
                    
                    await self._process_kpi_update(event)
                
        except Exception as e:
            self.logger.error(f"Error calculating real-time KPIs: {e}")
    
    # Additional helper methods would be implemented here...
    async def _store_anomaly_record(self, event: DashboardKPIEvent):
        """Store anomaly record in database"""
        pass
    
    async def _trigger_anomaly_alert(self, event: DashboardKPIEvent):
        """Trigger anomaly alert"""
        pass
    
    async def _trigger_threshold_alert(self, event: DashboardKPIEvent):
        """Trigger threshold breach alert"""
        pass
    
    async def _send_multi_channel_alert(self, event: DashboardKPIEvent):
        """Send alert through multiple channels"""
        pass
    
    async def _store_performance_metric(self, event: DashboardKPIEvent):
        """Store performance metric"""
        pass
    
    async def _trigger_performance_alert(self, event: DashboardKPIEvent):
        """Trigger performance alert"""
        pass
    
    async def _update_user_preferences(self, user_id: str, event: DashboardKPIEvent):
        """Update user preferences based on interaction"""
        pass
    
    def register_websocket_connection(self, user_id: str, websocket: Any):
        """Register WebSocket connection for real-time updates"""
        if user_id not in self.websocket_connections:
            self.websocket_connections[user_id] = set()
        self.websocket_connections[user_id].add(websocket)
    
    def unregister_websocket_connection(self, user_id: str, websocket: Any):
        """Unregister WebSocket connection"""
        if user_id in self.websocket_connections:
            self.websocket_connections[user_id].discard(websocket)
            if not self.websocket_connections[user_id]:
                del self.websocket_connections[user_id]
    
    def get_processing_stats(self) -> Dict[str, Any]:
        """Get current processing statistics"""
        return {
            "is_running": self.is_running,
            "processed_events": self.processed_events,
            "failed_events": self.failed_events,
            "success_rate": self.processed_events / (self.processed_events + self.failed_events) if (self.processed_events + self.failed_events) > 0 else 0,
            "last_processed": self.last_processed_timestamp.isoformat(),
            "cached_kpis_count": len(self.kpi_cache),
            "active_websocket_connections": sum(len(connections) for connections in self.websocket_connections.values()),
            "kpi_cache_keys": list(self.kpi_cache.keys())
        }


# Factory function
def create_real_time_dashboard_processor(config: Optional[DashboardProcessorConfig] = None) -> RealTimeDashboardProcessor:
    """Create RealTimeDashboardProcessor instance"""
    return RealTimeDashboardProcessor(config)


# Example usage
async def main():
    """Example usage of real-time dashboard processor"""
    processor = create_real_time_dashboard_processor()
    
    try:
        # Start processing
        await processor.start_processing()
    except KeyboardInterrupt:
        print("Processor interrupted by user")
    finally:
        await processor.stop_processing()


if __name__ == "__main__":
    asyncio.run(main())