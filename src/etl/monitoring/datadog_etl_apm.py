"""
DataDog APM Integration for ETL Pipeline Components
Provides comprehensive APM monitoring for ETL pipelines with distributed tracing
"""

import asyncio
import json
import time
import traceback
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Union, Tuple

import pandas as pd
from ddtrace import tracer
from ddtrace.ext import SpanTypes

from core.logging import get_logger
from monitoring.datadog_integration import DatadogMonitoring
from monitoring.datadog_apm_middleware import (
    DataDogTraceContext, trace_function, add_custom_tags, add_custom_metric
)

logger = get_logger(__name__)


class ETLAPMTracker:
    """
    Comprehensive APM tracking for ETL operations
    
    Features:
    - Bronze/Silver/Gold layer processing tracing
    - Data quality monitoring with APM
    - Pipeline performance and bottleneck identification
    - Distributed tracing across ETL stages
    - Data lineage tracking with trace correlation
    - Error tracking and data quality alerts
    """
    
    def __init__(self, service_name: str = "etl-pipeline", datadog_monitoring: Optional[DatadogMonitoring] = None):
        self.service_name = service_name
        self.datadog_monitoring = datadog_monitoring
        self.logger = get_logger(f"{__name__}.{service_name}")
        
        # Pipeline tracking
        self.active_pipelines = {}
        self.layer_statistics = {
            "bronze": {"records_processed": 0, "total_time": 0, "runs": 0},
            "silver": {"records_processed": 0, "total_time": 0, "runs": 0},
            "gold": {"records_processed": 0, "total_time": 0, "runs": 0}
        }
        self.data_quality_metrics = {}
        
    @asynccontextmanager
    async def trace_etl_operation(self, operation_type: str, layer: str, 
                                 operation_name: str, pipeline_id: Optional[str] = None,
                                 dataset_name: Optional[str] = None, 
                                 metadata: Optional[Dict[str, Any]] = None):
        """Context manager for tracing ETL operations."""
        
        span_name = f"etl.{layer}.{operation_type}"
        resource_name = f"{layer}.{operation_name}"
        
        tags = {
            "etl.operation_type": operation_type,
            "etl.layer": layer,
            "etl.operation_name": operation_name,
            "service.component": "etl"
        }
        
        if pipeline_id:
            tags["etl.pipeline_id"] = pipeline_id
        if dataset_name:
            tags["etl.dataset_name"] = dataset_name
        if metadata:
            for key, value in metadata.items():
                tags[f"etl.{key}"] = str(value)
        
        with tracer.trace(
            name=span_name,
            service=self.service_name,
            resource=resource_name,
            span_type=SpanTypes.CUSTOM
        ) as span:
            
            # Set tags
            for key, value in tags.items():
                span.set_tag(key, value)
            
            # Set start time and generate operation ID
            start_time = time.time()
            operation_id = str(uuid.uuid4())
            span.set_tag("etl.start_time", datetime.utcnow().isoformat())
            span.set_tag("etl.operation_id", operation_id)
            
            try:
                yield span, operation_id
                span.set_tag("etl.success", True)
                
            except Exception as e:
                span.set_error(e)
                span.set_tag("etl.success", False)
                span.set_tag("etl.error_type", type(e).__name__)
                span.set_tag("etl.error_message", str(e))
                self.logger.error(f"ETL operation failed: {layer}.{operation_type}.{operation_name} - {str(e)}")
                raise
                
            finally:
                # Calculate duration
                duration = time.time() - start_time
                span.set_metric("etl.duration_seconds", duration)
                span.set_tag("etl.end_time", datetime.utcnow().isoformat())
                
                # Update layer statistics
                if layer in self.layer_statistics:
                    self.layer_statistics[layer]["total_time"] += duration
                    self.layer_statistics[layer]["runs"] += 1
                
                # Send custom metrics
                if self.datadog_monitoring:
                    await self._send_etl_operation_metrics(operation_type, layer, operation_name, 
                                                         duration, pipeline_id, dataset_name, span.error)
    
    async def _send_etl_operation_metrics(self, operation_type: str, layer: str, operation_name: str,
                                        duration: float, pipeline_id: Optional[str] = None,
                                        dataset_name: Optional[str] = None, has_error: bool = False):
        """Send ETL operation metrics to DataDog."""
        try:
            tags = [
                f"operation_type:{operation_type}",
                f"layer:{layer}",
                f"operation_name:{operation_name}",
                f"service:{self.service_name}"
            ]
            
            if pipeline_id:
                tags.append(f"pipeline_id:{pipeline_id}")
            if dataset_name:
                tags.append(f"dataset:{dataset_name}")
            
            # Track operation count
            self.datadog_monitoring.counter("etl.operations.total", tags=tags)
            
            # Track operation duration
            self.datadog_monitoring.histogram("etl.operations.duration", duration * 1000, tags=tags)
            
            # Track errors
            if has_error:
                self.datadog_monitoring.counter("etl.operations.errors", tags=tags)
            
            # Track layer-specific metrics
            self.datadog_monitoring.histogram(f"etl.layer.{layer}.duration", duration * 1000, tags=tags)
            
        except Exception as e:
            self.logger.warning(f"Failed to send ETL operation metrics: {str(e)}")
    
    # Bronze Layer APM
    
    async def trace_bronze_ingestion(self, dataset_name: str, source_type: str, 
                                   pipeline_id: Optional[str] = None):
        """Context manager for tracing bronze layer data ingestion."""
        
        metadata = {
            "source_type": source_type,
            "ingestion_method": "batch"  # or "stream"
        }
        
        return self.trace_etl_operation(
            operation_type="ingestion",
            layer="bronze",
            operation_name="data_ingestion",
            pipeline_id=pipeline_id,
            dataset_name=dataset_name,
            metadata=metadata
        )
    
    @trace_function(operation_name="etl.bronze.data_validation", service_name="etl-pipeline")
    async def trace_bronze_validation(self, dataset_name: str, record_count: int,
                                    validation_rules: List[str], validation_results: Dict[str, Any]) -> Dict[str, Any]:
        """Trace bronze layer data validation."""
        
        passed_validations = sum(1 for result in validation_results.values() if result.get("passed", False))
        failed_validations = len(validation_results) - passed_validations
        validation_success_rate = passed_validations / len(validation_results) if validation_results else 1.0
        
        add_custom_tags({
            "etl.dataset_name": dataset_name,
            "etl.record_count": record_count,
            "etl.validation_rules_count": len(validation_rules),
            "etl.validations_passed": passed_validations,
            "etl.validations_failed": failed_validations,
            "etl.validation_success_rate": validation_success_rate
        })
        
        add_custom_metric("etl.bronze.record_count", float(record_count))
        add_custom_metric("etl.bronze.validation_success_rate", validation_success_rate)
        add_custom_metric("etl.bronze.validations_failed", float(failed_validations))
        
        if self.datadog_monitoring:
            tags = [f"dataset:{dataset_name}", "layer:bronze"]
            
            self.datadog_monitoring.gauge("etl.bronze.records", record_count, tags=tags)
            self.datadog_monitoring.gauge("etl.bronze.validation_success_rate", validation_success_rate, tags=tags)
            self.datadog_monitoring.gauge("etl.bronze.validations_failed", failed_validations, tags=tags)
            
            # Track individual validation results
            for rule_name, result in validation_results.items():
                validation_tags = tags + [f"validation_rule:{rule_name}"]
                if result.get("passed", False):
                    self.datadog_monitoring.counter("etl.bronze.validations.passed", tags=validation_tags)
                else:
                    self.datadog_monitoring.counter("etl.bronze.validations.failed", tags=validation_tags)
        
        return {
            "dataset_name": dataset_name,
            "record_count": record_count,
            "validation_results": validation_results,
            "validation_success_rate": validation_success_rate
        }
    
    @trace_function(operation_name="etl.bronze.schema_detection", service_name="etl-pipeline")
    async def trace_bronze_schema_detection(self, dataset_name: str, detected_schema: Dict[str, str],
                                          schema_changes: List[Dict[str, Any]]) -> Dict[str, Any]:
        """Trace bronze layer schema detection."""
        
        column_count = len(detected_schema)
        schema_changes_count = len(schema_changes)
        
        add_custom_tags({
            "etl.dataset_name": dataset_name,
            "etl.column_count": column_count,
            "etl.schema_changes_count": schema_changes_count
        })
        
        add_custom_metric("etl.bronze.schema_columns", float(column_count))
        add_custom_metric("etl.bronze.schema_changes", float(schema_changes_count))
        
        if self.datadog_monitoring:
            tags = [f"dataset:{dataset_name}", "layer:bronze"]
            
            self.datadog_monitoring.gauge("etl.bronze.schema_columns", column_count, tags=tags)
            self.datadog_monitoring.gauge("etl.bronze.schema_changes", schema_changes_count, tags=tags)
            
            # Track schema evolution
            if schema_changes_count > 0:
                self.datadog_monitoring.counter("etl.bronze.schema_evolution", tags=tags)
        
        return {
            "dataset_name": dataset_name,
            "detected_schema": detected_schema,
            "schema_changes": schema_changes,
            "column_count": column_count
        }
    
    # Silver Layer APM
    
    async def trace_silver_transformation(self, dataset_name: str, transformation_type: str,
                                        pipeline_id: Optional[str] = None):
        """Context manager for tracing silver layer transformations."""
        
        metadata = {
            "transformation_type": transformation_type,
            "data_quality_enforcement": True
        }
        
        return self.trace_etl_operation(
            operation_type="transformation",
            layer="silver",
            operation_name="data_transformation",
            pipeline_id=pipeline_id,
            dataset_name=dataset_name,
            metadata=metadata
        )
    
    @trace_function(operation_name="etl.silver.data_cleaning", service_name="etl-pipeline")
    async def trace_silver_cleaning(self, dataset_name: str, records_input: int, records_output: int,
                                  cleaning_operations: List[str], data_quality_score: float) -> Dict[str, Any]:
        """Trace silver layer data cleaning."""
        
        records_removed = records_input - records_output
        retention_rate = records_output / records_input if records_input > 0 else 1.0
        
        add_custom_tags({
            "etl.dataset_name": dataset_name,
            "etl.records_input": records_input,
            "etl.records_output": records_output,
            "etl.records_removed": records_removed,
            "etl.retention_rate": retention_rate,
            "etl.data_quality_score": data_quality_score,
            "etl.cleaning_operations": ",".join(cleaning_operations)
        })
        
        add_custom_metric("etl.silver.records_input", float(records_input))
        add_custom_metric("etl.silver.records_output", float(records_output))
        add_custom_metric("etl.silver.retention_rate", retention_rate)
        add_custom_metric("etl.silver.data_quality_score", data_quality_score)
        
        # Update layer statistics
        self.layer_statistics["silver"]["records_processed"] += records_output
        
        if self.datadog_monitoring:
            tags = [f"dataset:{dataset_name}", "layer:silver"]
            
            self.datadog_monitoring.gauge("etl.silver.records_input", records_input, tags=tags)
            self.datadog_monitoring.gauge("etl.silver.records_output", records_output, tags=tags)
            self.datadog_monitoring.gauge("etl.silver.retention_rate", retention_rate, tags=tags)
            self.datadog_monitoring.gauge("etl.silver.data_quality_score", data_quality_score, tags=tags)
        
        return {
            "dataset_name": dataset_name,
            "records_input": records_input,
            "records_output": records_output,
            "retention_rate": retention_rate,
            "data_quality_score": data_quality_score,
            "cleaning_operations": cleaning_operations
        }
    
    @trace_function(operation_name="etl.silver.enrichment", service_name="etl-pipeline")
    async def trace_silver_enrichment(self, dataset_name: str, enrichment_sources: List[str],
                                    records_enriched: int, enrichment_success_rate: float,
                                    external_api_calls: int) -> Dict[str, Any]:
        """Trace silver layer data enrichment."""
        
        add_custom_tags({
            "etl.dataset_name": dataset_name,
            "etl.enrichment_sources": ",".join(enrichment_sources),
            "etl.records_enriched": records_enriched,
            "etl.enrichment_success_rate": enrichment_success_rate,
            "etl.external_api_calls": external_api_calls
        })
        
        add_custom_metric("etl.silver.records_enriched", float(records_enriched))
        add_custom_metric("etl.silver.enrichment_success_rate", enrichment_success_rate)
        add_custom_metric("etl.silver.external_api_calls", float(external_api_calls))
        
        if self.datadog_monitoring:
            tags = [f"dataset:{dataset_name}", "layer:silver"]
            
            self.datadog_monitoring.gauge("etl.silver.records_enriched", records_enriched, tags=tags)
            self.datadog_monitoring.gauge("etl.silver.enrichment_success_rate", enrichment_success_rate, tags=tags)
            self.datadog_monitoring.gauge("etl.silver.external_api_calls", external_api_calls, tags=tags)
        
        return {
            "dataset_name": dataset_name,
            "enrichment_sources": enrichment_sources,
            "records_enriched": records_enriched,
            "enrichment_success_rate": enrichment_success_rate,
            "external_api_calls": external_api_calls
        }
    
    # Gold Layer APM
    
    async def trace_gold_aggregation(self, dataset_name: str, aggregation_type: str,
                                   pipeline_id: Optional[str] = None):
        """Context manager for tracing gold layer aggregations."""
        
        metadata = {
            "aggregation_type": aggregation_type,
            "business_ready": True
        }
        
        return self.trace_etl_operation(
            operation_type="aggregation",
            layer="gold",
            operation_name="data_aggregation",
            pipeline_id=pipeline_id,
            dataset_name=dataset_name,
            metadata=metadata
        )
    
    @trace_function(operation_name="etl.gold.star_schema_build", service_name="etl-pipeline")
    async def trace_gold_star_schema(self, dataset_name: str, fact_table_records: int,
                                   dimension_tables: Dict[str, int], aggregation_levels: List[str],
                                   data_freshness_minutes: float) -> Dict[str, Any]:
        """Trace gold layer star schema building."""
        
        total_dimension_records = sum(dimension_tables.values())
        dimension_count = len(dimension_tables)
        
        add_custom_tags({
            "etl.dataset_name": dataset_name,
            "etl.fact_table_records": fact_table_records,
            "etl.dimension_count": dimension_count,
            "etl.total_dimension_records": total_dimension_records,
            "etl.aggregation_levels": ",".join(aggregation_levels),
            "etl.data_freshness_minutes": data_freshness_minutes
        })
        
        add_custom_metric("etl.gold.fact_table_records", float(fact_table_records))
        add_custom_metric("etl.gold.dimension_count", float(dimension_count))
        add_custom_metric("etl.gold.total_dimension_records", float(total_dimension_records))
        add_custom_metric("etl.gold.data_freshness_minutes", data_freshness_minutes)
        
        # Update layer statistics
        self.layer_statistics["gold"]["records_processed"] += fact_table_records
        
        if self.datadog_monitoring:
            tags = [f"dataset:{dataset_name}", "layer:gold"]
            
            self.datadog_monitoring.gauge("etl.gold.fact_table_records", fact_table_records, tags=tags)
            self.datadog_monitoring.gauge("etl.gold.dimension_count", dimension_count, tags=tags)
            self.datadog_monitoring.gauge("etl.gold.data_freshness_minutes", data_freshness_minutes, tags=tags)
            
            # Track individual dimension tables
            for dim_name, dim_records in dimension_tables.items():
                dim_tags = tags + [f"dimension:{dim_name}"]
                self.datadog_monitoring.gauge("etl.gold.dimension_records", dim_records, tags=dim_tags)
        
        return {
            "dataset_name": dataset_name,
            "fact_table_records": fact_table_records,
            "dimension_tables": dimension_tables,
            "aggregation_levels": aggregation_levels,
            "data_freshness_minutes": data_freshness_minutes
        }
    
    @trace_function(operation_name="etl.gold.business_metrics", service_name="etl-pipeline")
    async def trace_gold_business_metrics(self, dataset_name: str, business_metrics: Dict[str, float],
                                        kpi_calculations: Dict[str, float]) -> Dict[str, Any]:
        """Trace gold layer business metrics calculation."""
        
        metrics_count = len(business_metrics)
        kpi_count = len(kpi_calculations)
        
        add_custom_tags({
            "etl.dataset_name": dataset_name,
            "etl.business_metrics_count": metrics_count,
            "etl.kpi_count": kpi_count
        })
        
        add_custom_metric("etl.gold.business_metrics_count", float(metrics_count))
        add_custom_metric("etl.gold.kpi_count", float(kpi_count))
        
        # Add individual metrics
        for metric_name, metric_value in business_metrics.items():
            add_custom_metric(f"etl.business.{metric_name}", metric_value)
        
        for kpi_name, kpi_value in kpi_calculations.items():
            add_custom_metric(f"etl.kpi.{kpi_name}", kpi_value)
        
        if self.datadog_monitoring:
            tags = [f"dataset:{dataset_name}", "layer:gold"]
            
            # Send business metrics
            for metric_name, metric_value in business_metrics.items():
                metric_tags = tags + [f"metric:{metric_name}"]
                self.datadog_monitoring.gauge(f"etl.business.{metric_name}", metric_value, tags=metric_tags)
            
            # Send KPI calculations
            for kpi_name, kpi_value in kpi_calculations.items():
                kpi_tags = tags + [f"kpi:{kpi_name}"]
                self.datadog_monitoring.gauge(f"etl.kpi.{kpi_name}", kpi_value, tags=kpi_tags)
        
        return {
            "dataset_name": dataset_name,
            "business_metrics": business_metrics,
            "kpi_calculations": kpi_calculations,
            "metrics_count": metrics_count,
            "kpi_count": kpi_count
        }
    
    # Data Quality APM
    
    @trace_function(operation_name="etl.data_quality.assessment", service_name="etl-pipeline")
    async def trace_data_quality_assessment(self, dataset_name: str, layer: str,
                                           quality_dimensions: Dict[str, float],
                                           quality_rules: List[str], 
                                           overall_quality_score: float) -> Dict[str, Any]:
        """Trace comprehensive data quality assessment."""
        
        rules_count = len(quality_rules)
        dimensions_count = len(quality_dimensions)
        
        add_custom_tags({
            "etl.dataset_name": dataset_name,
            "etl.layer": layer,
            "etl.quality_dimensions_count": dimensions_count,
            "etl.quality_rules_count": rules_count,
            "etl.overall_quality_score": overall_quality_score
        })
        
        add_custom_metric("etl.data_quality.overall_score", overall_quality_score)
        add_custom_metric("etl.data_quality.dimensions_count", float(dimensions_count))
        add_custom_metric("etl.data_quality.rules_count", float(rules_count))
        
        # Add individual quality dimensions
        for dimension, score in quality_dimensions.items():
            add_custom_metric(f"etl.data_quality.{dimension}", score)
        
        # Store quality metrics for trending
        quality_key = f"{dataset_name}_{layer}"
        if quality_key not in self.data_quality_metrics:
            self.data_quality_metrics[quality_key] = []
        
        self.data_quality_metrics[quality_key].append({
            "timestamp": datetime.utcnow(),
            "score": overall_quality_score,
            "dimensions": quality_dimensions.copy()
        })
        
        # Keep only last 100 measurements
        if len(self.data_quality_metrics[quality_key]) > 100:
            self.data_quality_metrics[quality_key] = self.data_quality_metrics[quality_key][-100:]
        
        if self.datadog_monitoring:
            tags = [f"dataset:{dataset_name}", f"layer:{layer}"]
            
            self.datadog_monitoring.gauge("etl.data_quality.overall_score", overall_quality_score, tags=tags)
            
            # Send individual quality dimensions
            for dimension, score in quality_dimensions.items():
                dimension_tags = tags + [f"dimension:{dimension}"]
                self.datadog_monitoring.gauge(f"etl.data_quality.dimension.{dimension}", score, tags=dimension_tags)
        
        return {
            "dataset_name": dataset_name,
            "layer": layer,
            "quality_dimensions": quality_dimensions,
            "quality_rules": quality_rules,
            "overall_quality_score": overall_quality_score,
            "assessment_timestamp": datetime.utcnow().isoformat()
        }
    
    # Pipeline Orchestration APM
    
    async def trace_pipeline_execution(self, pipeline_id: str, pipeline_name: str,
                                     stages: List[str], schedule_type: str = "batch"):
        """Context manager for tracing complete pipeline execution."""
        
        self.active_pipelines[pipeline_id] = {
            "pipeline_name": pipeline_name,
            "stages": stages,
            "schedule_type": schedule_type,
            "start_time": datetime.utcnow(),
            "stage_timings": {},
            "stage_status": {}
        }
        
        metadata = {
            "pipeline_name": pipeline_name,
            "stages_count": len(stages),
            "schedule_type": schedule_type,
            "stages": ",".join(stages)
        }
        
        return self.trace_etl_operation(
            operation_type="pipeline",
            layer="orchestration",
            operation_name=pipeline_name,
            pipeline_id=pipeline_id,
            metadata=metadata
        )
    
    @trace_function(operation_name="etl.pipeline.stage_execution", service_name="etl-pipeline")
    async def trace_pipeline_stage(self, pipeline_id: str, stage_name: str, stage_order: int,
                                 input_records: int, output_records: int, 
                                 stage_duration: float, stage_success: bool) -> Dict[str, Any]:
        """Trace individual pipeline stage execution."""
        
        throughput = output_records / stage_duration if stage_duration > 0 else 0
        
        add_custom_tags({
            "etl.pipeline_id": pipeline_id,
            "etl.stage_name": stage_name,
            "etl.stage_order": stage_order,
            "etl.input_records": input_records,
            "etl.output_records": output_records,
            "etl.stage_duration": stage_duration,
            "etl.stage_success": stage_success,
            "etl.throughput_records_per_second": throughput
        })
        
        add_custom_metric("etl.stage.input_records", float(input_records))
        add_custom_metric("etl.stage.output_records", float(output_records))
        add_custom_metric("etl.stage.duration_seconds", stage_duration)
        add_custom_metric("etl.stage.throughput_rps", throughput)
        
        # Update pipeline tracking
        if pipeline_id in self.active_pipelines:
            self.active_pipelines[pipeline_id]["stage_timings"][stage_name] = stage_duration
            self.active_pipelines[pipeline_id]["stage_status"][stage_name] = stage_success
        
        if self.datadog_monitoring:
            tags = [
                f"pipeline_id:{pipeline_id}",
                f"stage_name:{stage_name}",
                f"stage_order:{stage_order}",
                f"success:{stage_success}"
            ]
            
            self.datadog_monitoring.histogram("etl.stage.duration", stage_duration * 1000, tags=tags)
            self.datadog_monitoring.gauge("etl.stage.throughput_rps", throughput, tags=tags)
            self.datadog_monitoring.counter("etl.stage.executions", tags=tags)
            
            if not stage_success:
                self.datadog_monitoring.counter("etl.stage.failures", tags=tags)
        
        return {
            "pipeline_id": pipeline_id,
            "stage_name": stage_name,
            "stage_order": stage_order,
            "input_records": input_records,
            "output_records": output_records,
            "stage_duration": stage_duration,
            "stage_success": stage_success,
            "throughput_rps": throughput
        }
    
    # Performance and Analytics
    
    def get_etl_performance_summary(self) -> Dict[str, Any]:
        """Get comprehensive ETL performance summary."""
        
        # Calculate layer performance
        layer_performance = {}
        for layer, stats in self.layer_statistics.items():
            if stats["runs"] > 0:
                avg_duration = stats["total_time"] / stats["runs"]
                throughput = stats["records_processed"] / stats["total_time"] if stats["total_time"] > 0 else 0
                
                layer_performance[layer] = {
                    "total_records_processed": stats["records_processed"],
                    "total_runs": stats["runs"],
                    "total_time_seconds": stats["total_time"],
                    "avg_duration_seconds": avg_duration,
                    "throughput_records_per_second": throughput
                }
            else:
                layer_performance[layer] = {
                    "total_records_processed": 0,
                    "total_runs": 0,
                    "total_time_seconds": 0,
                    "avg_duration_seconds": 0,
                    "throughput_records_per_second": 0
                }
        
        # Calculate data quality trends
        quality_trends = {}
        for dataset_layer, measurements in self.data_quality_metrics.items():
            if measurements:
                recent_scores = [m["score"] for m in measurements[-10:]]  # Last 10 measurements
                quality_trends[dataset_layer] = {
                    "current_score": measurements[-1]["score"],
                    "avg_score_last_10": sum(recent_scores) / len(recent_scores),
                    "total_measurements": len(measurements),
                    "trend": self._calculate_quality_trend(measurements)
                }
        
        # Calculate pipeline status
        active_pipeline_count = len(self.active_pipelines)
        pipeline_summary = {}
        for pipeline_id, pipeline_data in self.active_pipelines.items():
            total_stages = len(pipeline_data["stages"])
            completed_stages = len(pipeline_data["stage_status"])
            successful_stages = sum(1 for success in pipeline_data["stage_status"].values() if success)
            
            pipeline_summary[pipeline_id] = {
                "name": pipeline_data["pipeline_name"],
                "total_stages": total_stages,
                "completed_stages": completed_stages,
                "successful_stages": successful_stages,
                "success_rate": successful_stages / completed_stages if completed_stages > 0 else 0,
                "duration_hours": (datetime.utcnow() - pipeline_data["start_time"]).total_seconds() / 3600
            }
        
        return {
            "service_name": self.service_name,
            "timestamp": datetime.utcnow().isoformat(),
            "layer_performance": layer_performance,
            "data_quality_trends": quality_trends,
            "active_pipelines": active_pipeline_count,
            "pipeline_summary": pipeline_summary
        }
    
    def _calculate_quality_trend(self, measurements: List[Dict[str, Any]]) -> str:
        """Calculate data quality trend from measurements."""
        if len(measurements) < 3:
            return "insufficient_data"
        
        recent_avg = sum(m["score"] for m in measurements[-3:]) / 3
        older_avg = sum(m["score"] for m in measurements[-6:-3]) / 3 if len(measurements) >= 6 else measurements[0]["score"]
        
        if recent_avg > older_avg * 1.05:
            return "improving"
        elif recent_avg < older_avg * 0.95:
            return "declining"
        else:
            return "stable"


# Global ETL APM tracker
_etl_apm_tracker: Optional[ETLAPMTracker] = None


def get_etl_apm_tracker(service_name: str = "etl-pipeline", 
                       datadog_monitoring: Optional[DatadogMonitoring] = None) -> ETLAPMTracker:
    """Get or create ETL APM tracker."""
    global _etl_apm_tracker
    
    if _etl_apm_tracker is None:
        _etl_apm_tracker = ETLAPMTracker(service_name, datadog_monitoring)
    
    return _etl_apm_tracker


# Convenience functions for common ETL operations

async def trace_bronze_processing_session(dataset_name: str, source_type: str, pipeline_id: Optional[str] = None):
    """Convenience function for tracing bronze layer processing."""
    tracker = get_etl_apm_tracker()
    return await tracker.trace_bronze_ingestion(dataset_name, source_type, pipeline_id)


async def trace_silver_processing_session(dataset_name: str, transformation_type: str, pipeline_id: Optional[str] = None):
    """Convenience function for tracing silver layer processing."""
    tracker = get_etl_apm_tracker()
    return await tracker.trace_silver_transformation(dataset_name, transformation_type, pipeline_id)


async def trace_gold_processing_session(dataset_name: str, aggregation_type: str, pipeline_id: Optional[str] = None):
    """Convenience function for tracing gold layer processing."""
    tracker = get_etl_apm_tracker()
    return await tracker.trace_gold_aggregation(dataset_name, aggregation_type, pipeline_id)


async def trace_pipeline_session(pipeline_id: str, pipeline_name: str, stages: List[str], schedule_type: str = "batch"):
    """Convenience function for tracing complete pipeline execution."""
    tracker = get_etl_apm_tracker()
    return await tracker.trace_pipeline_execution(pipeline_id, pipeline_name, stages, schedule_type)