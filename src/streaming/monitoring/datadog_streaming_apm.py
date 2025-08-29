"""
DataDog APM Integration for Streaming Data Processing
Provides comprehensive APM monitoring for Kafka, RabbitMQ, and real-time data pipelines
"""

import asyncio
import json
import time
import traceback
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union

from ddtrace import tracer
from ddtrace.ext import SpanTypes

from core.logging import get_logger
from monitoring.datadog_integration import DatadogMonitoring
from monitoring.datadog_distributed_tracing import get_distributed_tracer, trace_data_pipeline, TraceType, CorrelationLevel
from monitoring.datadog_error_tracking import get_error_tracker, capture_exception
from monitoring.datadog_profiling import get_datadog_profiler, profile_operation
from monitoring.datadog_business_metrics import get_business_metrics_tracker, track_kpi_value
from monitoring.datadog_apm_middleware import (
    DataDogTraceContext, trace_function, add_custom_tags, add_custom_metric
)

logger = get_logger(__name__)


class StreamingAPMTracker:
    """
    Comprehensive APM tracking for streaming operations
    
    Features:
    - Kafka producer/consumer tracing
    - RabbitMQ message flow monitoring
    - Real-time analytics processing
    - Stream processing pipeline monitoring
    - Message latency and throughput tracking
    - Consumer lag monitoring
    - Error tracking and dead letter queues
    """
    
    def __init__(self, service_name: str = "streaming-platform", datadog_monitoring: Optional[DatadogMonitoring] = None):
        self.service_name = service_name
        self.datadog_monitoring = datadog_monitoring
        self.logger = get_logger(f"{__name__}.{service_name}")
        
        # Streaming metrics tracking
        self.kafka_metrics = {
            "messages_produced": 0,
            "messages_consumed": 0,
            "producer_errors": 0,
            "consumer_errors": 0,
            "topics": set(),
            "partitions_active": set()
        }
        
        self.rabbitmq_metrics = {
            "messages_published": 0,
            "messages_consumed": 0,
            "exchanges": set(),
            "queues": set(),
            "routing_keys": set()
        }
        
        self.stream_processors = {}
        self.consumer_groups = {}
        
        # Initialize enhanced APM components
        self.distributed_tracer = get_distributed_tracer(f"{service_name}-streaming-tracing", datadog_monitoring)
        self.error_tracker = get_error_tracker(f"{service_name}-streaming-errors", datadog_monitoring)
        self.profiler = get_datadog_profiler(f"{service_name}-streaming-profiler", datadog_monitoring)
        self.business_metrics = get_business_metrics_tracker(f"{service_name}-streaming-business", datadog_monitoring)
        
        self.logger.info(f"Enhanced Streaming APM tracker initialized for {service_name}")
        
    @asynccontextmanager
    async def trace_streaming_operation(self, operation_type: str, component: str,
                                      operation_name: str, topic_or_queue: Optional[str] = None,
                                      message_id: Optional[str] = None, 
                                      metadata: Optional[Dict[str, Any]] = None):
        """Context manager for tracing streaming operations."""
        
        span_name = f"streaming.{component}.{operation_type}"
        resource_name = f"{component}.{operation_name}"
        
        tags = {
            "streaming.operation_type": operation_type,
            "streaming.component": component,
            "streaming.operation_name": operation_name,
            "service.component": "streaming"
        }
        
        if topic_or_queue:
            tags["streaming.topic_or_queue"] = topic_or_queue
        if message_id:
            tags["streaming.message_id"] = message_id
        if metadata:
            for key, value in metadata.items():
                tags[f"streaming.{key}"] = str(value)
        
        with tracer.trace(
            name=span_name,
            service=self.service_name,
            resource=resource_name,
            span_type=SpanTypes.CUSTOM
        ) as span:
            
            # Set tags
            for key, value in tags.items():
                span.set_tag(key, value)
            
            # Set start time
            start_time = time.time()
            operation_id = str(uuid.uuid4())
            span.set_tag("streaming.start_time", datetime.utcnow().isoformat())
            span.set_tag("streaming.operation_id", operation_id)
            
            try:
                yield span, operation_id
                span.set_tag("streaming.success", True)
                
            except Exception as e:
                span.set_error(e)
                span.set_tag("streaming.success", False)
                span.set_tag("streaming.error_type", type(e).__name__)
                span.set_tag("streaming.error_message", str(e))
                self.logger.error(f"Streaming operation failed: {component}.{operation_type}.{operation_name} - {str(e)}")
                raise
                
            finally:
                # Calculate duration
                duration = time.time() - start_time
                span.set_metric("streaming.duration_seconds", duration)
                span.set_tag("streaming.end_time", datetime.utcnow().isoformat())
                
                # Send custom metrics
                if self.datadog_monitoring:
                    await self._send_streaming_operation_metrics(operation_type, component, operation_name,
                                                               duration, topic_or_queue, message_id, span.error)
    
    async def _send_streaming_operation_metrics(self, operation_type: str, component: str, operation_name: str,
                                              duration: float, topic_or_queue: Optional[str] = None,
                                              message_id: Optional[str] = None, has_error: bool = False):
        """Send streaming operation metrics to DataDog."""
        try:
            tags = [
                f"operation_type:{operation_type}",
                f"component:{component}",
                f"operation_name:{operation_name}",
                f"service:{self.service_name}"
            ]
            
            if topic_or_queue:
                tags.append(f"topic_queue:{topic_or_queue}")
            
            # Track operation count
            self.datadog_monitoring.counter("streaming.operations.total", tags=tags)
            
            # Track operation duration
            self.datadog_monitoring.histogram("streaming.operations.duration", duration * 1000, tags=tags)
            
            # Track errors
            if has_error:
                self.datadog_monitoring.counter("streaming.operations.errors", tags=tags)
            
        except Exception as e:
            self.logger.warning(f"Failed to send streaming operation metrics: {str(e)}")
    
    # Kafka APM
    
    async def trace_kafka_producer(self, topic: str, partition: Optional[int] = None, 
                                 producer_id: Optional[str] = None):
        """Context manager for tracing Kafka producer operations."""
        
        metadata = {
            "partition": partition,
            "producer_id": producer_id or "default"
        }
        
        return self.trace_streaming_operation(
            operation_type="produce",
            component="kafka",
            operation_name="message_production",
            topic_or_queue=topic,
            metadata=metadata
        )
    
    @trace_function(operation_name="streaming.kafka.message_send", service_name="streaming-platform")
    async def trace_kafka_message_send(self, topic: str, partition: int, offset: int,
                                     message_size: int, key: Optional[str] = None,
                                     headers: Optional[Dict[str, str]] = None) -> Dict[str, Any]:
        """Trace individual Kafka message sending."""
        
        message_id = f"{topic}_{partition}_{offset}"
        
        add_custom_tags({
            "streaming.topic": topic,
            "streaming.partition": partition,
            "streaming.offset": offset,
            "streaming.message_size": message_size,
            "streaming.has_key": key is not None,
            "streaming.headers_count": len(headers) if headers else 0
        })
        
        add_custom_metric("streaming.kafka.message_size", float(message_size))
        
        # Update metrics
        self.kafka_metrics["messages_produced"] += 1
        self.kafka_metrics["topics"].add(topic)
        self.kafka_metrics["partitions_active"].add(f"{topic}_{partition}")
        
        if self.datadog_monitoring:
            tags = [f"topic:{topic}", f"partition:{partition}"]
            
            self.datadog_monitoring.counter("streaming.kafka.messages.produced", tags=tags)
            self.datadog_monitoring.histogram("streaming.kafka.message_size", message_size, tags=tags)
            
            if key:
                tags.append("has_key:true")
            else:
                tags.append("has_key:false")
            
            self.datadog_monitoring.counter("streaming.kafka.message_characteristics", tags=tags)
        
        return {
            "message_id": message_id,
            "topic": topic,
            "partition": partition,
            "offset": offset,
            "message_size": message_size,
            "has_key": key is not None,
            "headers_count": len(headers) if headers else 0
        }
    
    async def trace_kafka_consumer(self, topic: str, consumer_group: str,
                                 consumer_id: Optional[str] = None):
        """Context manager for tracing Kafka consumer operations."""
        
        metadata = {
            "consumer_group": consumer_group,
            "consumer_id": consumer_id or "default"
        }
        
        if consumer_group not in self.consumer_groups:
            self.consumer_groups[consumer_group] = {
                "topics": set(),
                "consumers": set(),
                "messages_consumed": 0,
                "last_commit": datetime.utcnow(),
                "lag_monitoring": {}
            }
        
        self.consumer_groups[consumer_group]["topics"].add(topic)
        if consumer_id:
            self.consumer_groups[consumer_group]["consumers"].add(consumer_id)
        
        return self.trace_streaming_operation(
            operation_type="consume",
            component="kafka",
            operation_name="message_consumption",
            topic_or_queue=topic,
            metadata=metadata
        )
    
    @trace_function(operation_name="streaming.kafka.message_consume", service_name="streaming-platform")
    async def trace_kafka_message_consume(self, topic: str, partition: int, offset: int,
                                        consumer_group: str, lag: int, message_size: int,
                                        processing_time_ms: Optional[float] = None) -> Dict[str, Any]:
        """Trace individual Kafka message consumption."""
        
        message_id = f"{topic}_{partition}_{offset}"
        
        add_custom_tags({
            "streaming.topic": topic,
            "streaming.partition": partition,
            "streaming.offset": offset,
            "streaming.consumer_group": consumer_group,
            "streaming.lag": lag,
            "streaming.message_size": message_size
        })
        
        if processing_time_ms is not None:
            add_custom_tags({"streaming.processing_time_ms": processing_time_ms})
            add_custom_metric("streaming.kafka.processing_time_ms", processing_time_ms)
        
        add_custom_metric("streaming.kafka.consumer_lag", float(lag))
        add_custom_metric("streaming.kafka.message_size", float(message_size))
        
        # Update metrics
        self.kafka_metrics["messages_consumed"] += 1
        if consumer_group in self.consumer_groups:
            self.consumer_groups[consumer_group]["messages_consumed"] += 1
            self.consumer_groups[consumer_group]["lag_monitoring"][f"{topic}_{partition}"] = {
                "lag": lag,
                "timestamp": datetime.utcnow()
            }
        
        if self.datadog_monitoring:
            tags = [
                f"topic:{topic}",
                f"partition:{partition}",
                f"consumer_group:{consumer_group}"
            ]
            
            self.datadog_monitoring.counter("streaming.kafka.messages.consumed", tags=tags)
            self.datadog_monitoring.gauge("streaming.kafka.consumer_lag", lag, tags=tags)
            self.datadog_monitoring.histogram("streaming.kafka.message_size", message_size, tags=tags)
            
            if processing_time_ms is not None:
                self.datadog_monitoring.histogram("streaming.kafka.processing_time", processing_time_ms, tags=tags)
        
        return {
            "message_id": message_id,
            "topic": topic,
            "partition": partition,
            "offset": offset,
            "consumer_group": consumer_group,
            "lag": lag,
            "message_size": message_size,
            "processing_time_ms": processing_time_ms
        }
    
    @trace_function(operation_name="streaming.kafka.batch_process", service_name="streaming-platform")
    async def trace_kafka_batch_processing(self, topic: str, consumer_group: str,
                                         batch_size: int, processing_time_ms: float,
                                         success_count: int, error_count: int) -> Dict[str, Any]:
        """Trace Kafka batch message processing."""
        
        success_rate = success_count / batch_size if batch_size > 0 else 1.0
        throughput = batch_size / (processing_time_ms / 1000) if processing_time_ms > 0 else 0
        
        add_custom_tags({
            "streaming.topic": topic,
            "streaming.consumer_group": consumer_group,
            "streaming.batch_size": batch_size,
            "streaming.success_count": success_count,
            "streaming.error_count": error_count,
            "streaming.success_rate": success_rate,
            "streaming.throughput_mps": throughput
        })
        
        add_custom_metric("streaming.kafka.batch_size", float(batch_size))
        add_custom_metric("streaming.kafka.batch_success_rate", success_rate)
        add_custom_metric("streaming.kafka.batch_throughput_mps", throughput)
        
        if self.datadog_monitoring:
            tags = [f"topic:{topic}", f"consumer_group:{consumer_group}"]
            
            self.datadog_monitoring.histogram("streaming.kafka.batch_size", batch_size, tags=tags)
            self.datadog_monitoring.histogram("streaming.kafka.batch_processing_time", processing_time_ms, tags=tags)
            self.datadog_monitoring.gauge("streaming.kafka.batch_success_rate", success_rate, tags=tags)
            self.datadog_monitoring.gauge("streaming.kafka.throughput_mps", throughput, tags=tags)
            
            if error_count > 0:
                self.datadog_monitoring.counter("streaming.kafka.batch_errors", error_count, tags=tags)
        
        return {
            "topic": topic,
            "consumer_group": consumer_group,
            "batch_size": batch_size,
            "processing_time_ms": processing_time_ms,
            "success_count": success_count,
            "error_count": error_count,
            "success_rate": success_rate,
            "throughput_mps": throughput
        }
    
    # RabbitMQ APM
    
    async def trace_rabbitmq_publisher(self, exchange: str, routing_key: str,
                                     queue: Optional[str] = None):
        """Context manager for tracing RabbitMQ publisher operations."""
        
        metadata = {
            "exchange": exchange,
            "routing_key": routing_key,
            "queue": queue
        }
        
        return self.trace_streaming_operation(
            operation_type="publish",
            component="rabbitmq",
            operation_name="message_publish",
            topic_or_queue=queue or f"{exchange}_{routing_key}",
            metadata=metadata
        )
    
    @trace_function(operation_name="streaming.rabbitmq.message_publish", service_name="streaming-platform")
    async def trace_rabbitmq_message_publish(self, exchange: str, routing_key: str,
                                           message_size: int, priority: Optional[int] = None,
                                           expiration: Optional[int] = None,
                                           persistent: bool = True) -> Dict[str, Any]:
        """Trace individual RabbitMQ message publishing."""
        
        message_id = str(uuid.uuid4())
        
        add_custom_tags({
            "streaming.exchange": exchange,
            "streaming.routing_key": routing_key,
            "streaming.message_size": message_size,
            "streaming.priority": priority or 0,
            "streaming.persistent": persistent,
            "streaming.has_expiration": expiration is not None
        })
        
        add_custom_metric("streaming.rabbitmq.message_size", float(message_size))
        if priority is not None:
            add_custom_metric("streaming.rabbitmq.message_priority", float(priority))
        
        # Update metrics
        self.rabbitmq_metrics["messages_published"] += 1
        self.rabbitmq_metrics["exchanges"].add(exchange)
        self.rabbitmq_metrics["routing_keys"].add(routing_key)
        
        if self.datadog_monitoring:
            tags = [f"exchange:{exchange}", f"routing_key:{routing_key}", f"persistent:{persistent}"]
            
            self.datadog_monitoring.counter("streaming.rabbitmq.messages.published", tags=tags)
            self.datadog_monitoring.histogram("streaming.rabbitmq.message_size", message_size, tags=tags)
            
            if priority is not None:
                priority_tags = tags + [f"priority:{priority}"]
                self.datadog_monitoring.counter("streaming.rabbitmq.priority_messages", tags=priority_tags)
        
        return {
            "message_id": message_id,
            "exchange": exchange,
            "routing_key": routing_key,
            "message_size": message_size,
            "priority": priority,
            "persistent": persistent,
            "expiration": expiration
        }
    
    async def trace_rabbitmq_consumer(self, queue: str, exchange: str,
                                    consumer_tag: Optional[str] = None):
        """Context manager for tracing RabbitMQ consumer operations."""
        
        metadata = {
            "exchange": exchange,
            "consumer_tag": consumer_tag or "default"
        }
        
        return self.trace_streaming_operation(
            operation_type="consume",
            component="rabbitmq",
            operation_name="message_consume",
            topic_or_queue=queue,
            metadata=metadata
        )
    
    @trace_function(operation_name="streaming.rabbitmq.message_consume", service_name="streaming-platform")
    async def trace_rabbitmq_message_consume(self, queue: str, exchange: str, routing_key: str,
                                           message_size: int, delivery_tag: int,
                                           processing_time_ms: Optional[float] = None,
                                           ack: bool = True, requeue: bool = False) -> Dict[str, Any]:
        """Trace individual RabbitMQ message consumption."""
        
        add_custom_tags({
            "streaming.queue": queue,
            "streaming.exchange": exchange,
            "streaming.routing_key": routing_key,
            "streaming.message_size": message_size,
            "streaming.delivery_tag": delivery_tag,
            "streaming.ack": ack,
            "streaming.requeue": requeue
        })
        
        if processing_time_ms is not None:
            add_custom_tags({"streaming.processing_time_ms": processing_time_ms})
            add_custom_metric("streaming.rabbitmq.processing_time_ms", processing_time_ms)
        
        add_custom_metric("streaming.rabbitmq.message_size", float(message_size))
        
        # Update metrics
        self.rabbitmq_metrics["messages_consumed"] += 1
        self.rabbitmq_metrics["queues"].add(queue)
        
        if self.datadog_monitoring:
            tags = [
                f"queue:{queue}",
                f"exchange:{exchange}",
                f"routing_key:{routing_key}",
                f"ack:{ack}",
                f"requeue:{requeue}"
            ]
            
            self.datadog_monitoring.counter("streaming.rabbitmq.messages.consumed", tags=tags)
            self.datadog_monitoring.histogram("streaming.rabbitmq.message_size", message_size, tags=tags)
            
            if processing_time_ms is not None:
                self.datadog_monitoring.histogram("streaming.rabbitmq.processing_time", processing_time_ms, tags=tags)
            
            if not ack:
                self.datadog_monitoring.counter("streaming.rabbitmq.messages.nack", tags=tags)
            if requeue:
                self.datadog_monitoring.counter("streaming.rabbitmq.messages.requeue", tags=tags)
        
        return {
            "queue": queue,
            "exchange": exchange,
            "routing_key": routing_key,
            "message_size": message_size,
            "delivery_tag": delivery_tag,
            "processing_time_ms": processing_time_ms,
            "ack": ack,
            "requeue": requeue
        }
    
    # Stream Processing APM
    
    async def trace_stream_processor(self, processor_name: str, processor_type: str,
                                   input_sources: List[str], output_sinks: List[str]):
        """Context manager for tracing stream processing operations."""
        
        processor_id = str(uuid.uuid4())
        self.stream_processors[processor_id] = {
            "processor_name": processor_name,
            "processor_type": processor_type,
            "input_sources": input_sources,
            "output_sinks": output_sinks,
            "start_time": datetime.utcnow(),
            "messages_processed": 0,
            "errors": 0
        }
        
        metadata = {
            "processor_type": processor_type,
            "processor_id": processor_id,
            "input_sources": ",".join(input_sources),
            "output_sinks": ",".join(output_sinks)
        }
        
        return self.trace_streaming_operation(
            operation_type="process",
            component="stream_processor",
            operation_name=processor_name,
            metadata=metadata
        )
    
    @trace_function(operation_name="streaming.processor.message_transform", service_name="streaming-platform")
    async def trace_stream_transformation(self, processor_name: str, transformation_type: str,
                                        input_message_count: int, output_message_count: int,
                                        transformation_time_ms: float,
                                        schema_evolution: bool = False) -> Dict[str, Any]:
        """Trace stream message transformation."""
        
        message_ratio = output_message_count / input_message_count if input_message_count > 0 else 0
        throughput = input_message_count / (transformation_time_ms / 1000) if transformation_time_ms > 0 else 0
        
        add_custom_tags({
            "streaming.processor_name": processor_name,
            "streaming.transformation_type": transformation_type,
            "streaming.input_message_count": input_message_count,
            "streaming.output_message_count": output_message_count,
            "streaming.message_ratio": message_ratio,
            "streaming.throughput_mps": throughput,
            "streaming.schema_evolution": schema_evolution
        })
        
        add_custom_metric("streaming.processor.input_messages", float(input_message_count))
        add_custom_metric("streaming.processor.output_messages", float(output_message_count))
        add_custom_metric("streaming.processor.message_ratio", message_ratio)
        add_custom_metric("streaming.processor.throughput_mps", throughput)
        
        if self.datadog_monitoring:
            tags = [
                f"processor:{processor_name}",
                f"transformation_type:{transformation_type}",
                f"schema_evolution:{schema_evolution}"
            ]
            
            self.datadog_monitoring.gauge("streaming.processor.input_messages", input_message_count, tags=tags)
            self.datadog_monitoring.gauge("streaming.processor.output_messages", output_message_count, tags=tags)
            self.datadog_monitoring.gauge("streaming.processor.message_ratio", message_ratio, tags=tags)
            self.datadog_monitoring.histogram("streaming.processor.transformation_time", transformation_time_ms, tags=tags)
            self.datadog_monitoring.gauge("streaming.processor.throughput_mps", throughput, tags=tags)
            
            if schema_evolution:
                self.datadog_monitoring.counter("streaming.processor.schema_evolution", tags=tags)
        
        return {
            "processor_name": processor_name,
            "transformation_type": transformation_type,
            "input_message_count": input_message_count,
            "output_message_count": output_message_count,
            "message_ratio": message_ratio,
            "transformation_time_ms": transformation_time_ms,
            "throughput_mps": throughput,
            "schema_evolution": schema_evolution
        }
    
    @trace_function(operation_name="streaming.processor.aggregation", service_name="streaming-platform")
    async def trace_stream_aggregation(self, processor_name: str, aggregation_type: str,
                                     window_size_ms: int, window_count: int,
                                     records_aggregated: int, output_records: int) -> Dict[str, Any]:
        """Trace stream aggregation operations."""
        
        aggregation_ratio = records_aggregated / output_records if output_records > 0 else 0
        records_per_window = records_aggregated / window_count if window_count > 0 else 0
        
        add_custom_tags({
            "streaming.processor_name": processor_name,
            "streaming.aggregation_type": aggregation_type,
            "streaming.window_size_ms": window_size_ms,
            "streaming.window_count": window_count,
            "streaming.records_aggregated": records_aggregated,
            "streaming.output_records": output_records,
            "streaming.aggregation_ratio": aggregation_ratio,
            "streaming.records_per_window": records_per_window
        })
        
        add_custom_metric("streaming.aggregation.records_aggregated", float(records_aggregated))
        add_custom_metric("streaming.aggregation.output_records", float(output_records))
        add_custom_metric("streaming.aggregation.ratio", aggregation_ratio)
        add_custom_metric("streaming.aggregation.records_per_window", records_per_window)
        
        if self.datadog_monitoring:
            tags = [
                f"processor:{processor_name}",
                f"aggregation_type:{aggregation_type}",
                f"window_size_ms:{window_size_ms}"
            ]
            
            self.datadog_monitoring.gauge("streaming.aggregation.records_aggregated", records_aggregated, tags=tags)
            self.datadog_monitoring.gauge("streaming.aggregation.output_records", output_records, tags=tags)
            self.datadog_monitoring.gauge("streaming.aggregation.ratio", aggregation_ratio, tags=tags)
            self.datadog_monitoring.gauge("streaming.aggregation.window_count", window_count, tags=tags)
        
        return {
            "processor_name": processor_name,
            "aggregation_type": aggregation_type,
            "window_size_ms": window_size_ms,
            "window_count": window_count,
            "records_aggregated": records_aggregated,
            "output_records": output_records,
            "aggregation_ratio": aggregation_ratio,
            "records_per_window": records_per_window
        }
    
    # Real-time Analytics APM
    
    @trace_function(operation_name="streaming.analytics.real_time_calculation", service_name="streaming-platform")
    async def trace_realtime_analytics(self, analytics_type: str, calculation_name: str,
                                     data_points: int, calculation_time_ms: float,
                                     result_value: Optional[float] = None) -> Dict[str, Any]:
        """Trace real-time analytics calculations."""
        
        calculation_throughput = data_points / (calculation_time_ms / 1000) if calculation_time_ms > 0 else 0
        
        add_custom_tags({
            "streaming.analytics_type": analytics_type,
            "streaming.calculation_name": calculation_name,
            "streaming.data_points": data_points,
            "streaming.calculation_throughput": calculation_throughput
        })
        
        if result_value is not None:
            add_custom_tags({"streaming.result_value": result_value})
            add_custom_metric(f"streaming.analytics.{calculation_name}", result_value)
        
        add_custom_metric("streaming.analytics.data_points", float(data_points))
        add_custom_metric("streaming.analytics.calculation_throughput", calculation_throughput)
        
        if self.datadog_monitoring:
            tags = [
                f"analytics_type:{analytics_type}",
                f"calculation:{calculation_name}"
            ]
            
            self.datadog_monitoring.gauge("streaming.analytics.data_points", data_points, tags=tags)
            self.datadog_monitoring.histogram("streaming.analytics.calculation_time", calculation_time_ms, tags=tags)
            self.datadog_monitoring.gauge("streaming.analytics.throughput", calculation_throughput, tags=tags)
            
            if result_value is not None:
                self.datadog_monitoring.gauge(f"streaming.analytics.result.{calculation_name}", result_value, tags=tags)
        
        return {
            "analytics_type": analytics_type,
            "calculation_name": calculation_name,
            "data_points": data_points,
            "calculation_time_ms": calculation_time_ms,
            "calculation_throughput": calculation_throughput,
            "result_value": result_value
        }
    
    # Performance and Analytics
    
    def get_streaming_performance_summary(self) -> Dict[str, Any]:
        """Get comprehensive streaming performance summary."""
        
        # Kafka statistics
        kafka_summary = {
            "messages_produced": self.kafka_metrics["messages_produced"],
            "messages_consumed": self.kafka_metrics["messages_consumed"],
            "producer_errors": self.kafka_metrics["producer_errors"],
            "consumer_errors": self.kafka_metrics["consumer_errors"],
            "active_topics": len(self.kafka_metrics["topics"]),
            "active_partitions": len(self.kafka_metrics["partitions_active"]),
            "topics": list(self.kafka_metrics["topics"]),
            "message_balance": self.kafka_metrics["messages_produced"] - self.kafka_metrics["messages_consumed"]
        }
        
        # RabbitMQ statistics
        rabbitmq_summary = {
            "messages_published": self.rabbitmq_metrics["messages_published"],
            "messages_consumed": self.rabbitmq_metrics["messages_consumed"],
            "active_exchanges": len(self.rabbitmq_metrics["exchanges"]),
            "active_queues": len(self.rabbitmq_metrics["queues"]),
            "unique_routing_keys": len(self.rabbitmq_metrics["routing_keys"]),
            "exchanges": list(self.rabbitmq_metrics["exchanges"]),
            "queues": list(self.rabbitmq_metrics["queues"]),
            "message_balance": self.rabbitmq_metrics["messages_published"] - self.rabbitmq_metrics["messages_consumed"]
        }
        
        # Consumer group statistics
        consumer_group_summary = {}
        for group_name, group_data in self.consumer_groups.items():
            current_time = datetime.utcnow()
            
            # Calculate average lag
            current_lags = [
                lag_info["lag"] for lag_info in group_data["lag_monitoring"].values()
                if (current_time - lag_info["timestamp"]).total_seconds() < 300  # Last 5 minutes
            ]
            avg_lag = sum(current_lags) / len(current_lags) if current_lags else 0
            max_lag = max(current_lags) if current_lags else 0
            
            consumer_group_summary[group_name] = {
                "topics": list(group_data["topics"]),
                "consumer_count": len(group_data["consumers"]),
                "messages_consumed": group_data["messages_consumed"],
                "avg_lag": avg_lag,
                "max_lag": max_lag,
                "partitions_monitored": len(group_data["lag_monitoring"]),
                "last_commit_minutes_ago": (current_time - group_data["last_commit"]).total_seconds() / 60
            }
        
        # Stream processor statistics
        processor_summary = {}
        for processor_id, processor_data in self.stream_processors.items():
            duration_hours = (datetime.utcnow() - processor_data["start_time"]).total_seconds() / 3600
            throughput = processor_data["messages_processed"] / duration_hours if duration_hours > 0 else 0
            error_rate = processor_data["errors"] / processor_data["messages_processed"] if processor_data["messages_processed"] > 0 else 0
            
            processor_summary[processor_id] = {
                "name": processor_data["processor_name"],
                "type": processor_data["processor_type"],
                "input_sources": processor_data["input_sources"],
                "output_sinks": processor_data["output_sinks"],
                "messages_processed": processor_data["messages_processed"],
                "errors": processor_data["errors"],
                "error_rate": error_rate,
                "throughput_messages_per_hour": throughput,
                "duration_hours": duration_hours
            }
        
        return {
            "service_name": self.service_name,
            "timestamp": datetime.utcnow().isoformat(),
            "kafka": kafka_summary,
            "rabbitmq": rabbitmq_summary,
            "consumer_groups": consumer_group_summary,
            "stream_processors": processor_summary,
            "system_health": {
                "total_active_streams": len(self.stream_processors),
                "total_consumer_groups": len(self.consumer_groups),
                "overall_message_throughput": (
                    kafka_summary["messages_produced"] + kafka_summary["messages_consumed"] +
                    rabbitmq_summary["messages_published"] + rabbitmq_summary["messages_consumed"]
                )
            }
        }


# Global streaming APM tracker
_streaming_apm_tracker: Optional[StreamingAPMTracker] = None


def get_streaming_apm_tracker(service_name: str = "streaming-platform", 
                            datadog_monitoring: Optional[DatadogMonitoring] = None) -> StreamingAPMTracker:
    """Get or create streaming APM tracker."""
    global _streaming_apm_tracker
    
    if _streaming_apm_tracker is None:
        _streaming_apm_tracker = StreamingAPMTracker(service_name, datadog_monitoring)
    
    return _streaming_apm_tracker


# Convenience functions for common streaming operations

async def trace_kafka_producer_session(topic: str, partition: Optional[int] = None, producer_id: Optional[str] = None):
    """Convenience function for tracing Kafka producer operations."""
    tracker = get_streaming_apm_tracker()
    return await tracker.trace_kafka_producer(topic, partition, producer_id)


async def trace_kafka_consumer_session(topic: str, consumer_group: str, consumer_id: Optional[str] = None):
    """Convenience function for tracing Kafka consumer operations."""
    tracker = get_streaming_apm_tracker()
    return await tracker.trace_kafka_consumer(topic, consumer_group, consumer_id)


async def trace_rabbitmq_publisher_session(exchange: str, routing_key: str, queue: Optional[str] = None):
    """Convenience function for tracing RabbitMQ publisher operations."""
    tracker = get_streaming_apm_tracker()
    return await tracker.trace_rabbitmq_publisher(exchange, routing_key, queue)


async def trace_rabbitmq_consumer_session(queue: str, exchange: str, consumer_tag: Optional[str] = None):
    """Convenience function for tracing RabbitMQ consumer operations."""
    tracker = get_streaming_apm_tracker()
    return await tracker.trace_rabbitmq_consumer(queue, exchange, consumer_tag)


async def trace_stream_processor_session(processor_name: str, processor_type: str,
                                       input_sources: List[str], output_sinks: List[str]):
    """Convenience function for tracing stream processor operations."""
    tracker = get_streaming_apm_tracker()
    return await tracker.trace_stream_processor(processor_name, processor_type, input_sources, output_sinks)