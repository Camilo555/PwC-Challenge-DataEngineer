"""
Base ETL Processor Framework
Provides a unified interface and common functionality for all ETL processors.
"""

from __future__ import annotations

import gc
import json
import os
import time
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

import psutil

from core.logging import get_logger
from monitoring import AlertSeverity, get_metrics_collector, trigger_custom_alert

# Import schema evolution support
try:
    from etl.schema_evolution import (
        create_schema_orchestrator,
        SchemaEvolutionOrchestrator,
        MedallionLayer
    )
    SCHEMA_EVOLUTION_AVAILABLE = True
except ImportError:
    SCHEMA_EVOLUTION_AVAILABLE = False
    SchemaEvolutionOrchestrator = None


class ProcessingEngine(Enum):
    """Supported processing engines"""

    PANDAS = "pandas"
    SPARK = "spark"
    DASK = "dask"


class ProcessingStage(Enum):
    """ETL processing stages"""

    BRONZE = "bronze"
    SILVER = "silver"
    GOLD = "gold"
    VALIDATION = "validation"
    ENRICHMENT = "enrichment"


@dataclass
class ProcessingResult:
    """Standard result structure for all processors"""

    success: bool
    records_processed: int
    records_failed: int
    processing_time_seconds: float
    data_quality_score: float
    error_count: int
    warnings_count: int
    output_path: str | None = None
    metadata: dict[str, Any] | None = None
    errors: list[str] | None = None
    warnings: list[str] | None = None


@dataclass
class ProcessingConfig:
    """Configuration for ETL processing"""

    engine: ProcessingEngine
    stage: ProcessingStage
    input_path: str
    output_path: str
    enable_monitoring: bool = True
    enable_quality_checks: bool = True
    enable_caching: bool = False
    batch_size: int | None = None
    parallel_processing: bool = False
    compression: str | None = None
    partition_by: list[str] | None = None
    quality_threshold: float = 0.95
    max_errors: int = 1000
    custom_config: dict[str, Any] | None = None

    # Memory Management Configuration
    memory_limit_mb: int = 4096  # Default 4GB memory limit
    memory_warning_threshold: float = 0.8  # Warn at 80% of limit
    memory_check_interval: int = 30  # Check memory every 30 seconds
    enable_memory_monitoring: bool = True
    auto_garbage_collection: bool = True
    chunk_size_rows: int = 10000  # Process data in chunks for large datasets


class BaseETLProcessor(ABC):
    """
    Base class for all ETL processors providing common functionality and interface.

    This class provides:
    - Standardized processing interface
    - Monitoring and metrics collection
    - Error handling and logging
    - Data quality validation
    - Configuration management
    """

    def __init__(self, config: ProcessingConfig):
        self.config = config
        self.logger = get_logger(self.__class__.__name__)
        self.metrics_collector = get_metrics_collector() if config.enable_monitoring else None
        self.start_time: datetime | None = None
        self.errors: list[str] = []
        self.warnings: list[str] = []

        # Memory management attributes
        self._memory_monitor_active = False
        self._last_memory_check = time.time()
        self._initial_memory_usage = (
            self._get_memory_usage_mb() if config.enable_memory_monitoring else 0
        )

        # Schema evolution support
        self._schema_orchestrator: SchemaEvolutionOrchestrator | None = None
        if SCHEMA_EVOLUTION_AVAILABLE and getattr(config, 'enable_schema_evolution', False):
            try:
                self._schema_orchestrator = create_schema_orchestrator()
                self.logger.info("Schema evolution support enabled")
            except Exception as e:
                self.logger.warning(f"Failed to initialize schema evolution: {e}")

        # Initialize processing state
        self._initialize_processor()

    def _initialize_processor(self):
        """Initialize processor-specific components"""
        self.logger.info(
            f"Initializing {self.__class__.__name__} with {self.config.engine.value} engine"
        )

        # Create output directories
        output_dir = Path(self.config.output_path)
        output_dir.mkdir(parents=True, exist_ok=True)

        # Initialize monitoring if enabled
        if self.config.enable_monitoring and self.metrics_collector:
            self.logger.info("Monitoring enabled for processor")

    @abstractmethod
    def process(self) -> ProcessingResult:
        """
        Main processing method to be implemented by subclasses.

        Returns:
            ProcessingResult: Standardized processing result
        """
        pass

    @abstractmethod
    def validate_input(self) -> bool:
        """
        Validate input data and configuration.

        Returns:
            bool: True if validation passes
        """
        pass

    def run(self) -> ProcessingResult:
        """
        Execute the complete processing pipeline with error handling and monitoring.

        Returns:
            ProcessingResult: Complete processing result
        """
        self.start_time = datetime.now()
        self.logger.info(f"Starting {self.config.stage.value} processing")

        try:
            # Pre-processing validation
            if not self.validate_input():
                return self._create_failure_result("Input validation failed")

            # Execute main processing
            result = self.process()

            # Post-processing validation
            if result.success:
                result = self._post_process_validation(result)

            # Record metrics
            self._record_metrics(result)

            # Log completion
            self._log_completion(result)

            return result

        except Exception as e:
            self.logger.error(f"Processing failed with exception: {e}", exc_info=True)
            self.errors.append(str(e))

            # Send alert for critical failures
            if self.config.enable_monitoring:
                trigger_custom_alert(
                    title=f"{self.config.stage.value.title()} Processing Failed",
                    description=f"Error in {self.__class__.__name__}: {str(e)}",
                    severity=AlertSeverity.HIGH,
                    source="etl_processor",
                )

            return self._create_failure_result(str(e))

    def _post_process_validation(self, result: ProcessingResult) -> ProcessingResult:
        """Validate processing results and output"""
        try:
            # Check output file exists if path provided
            if result.output_path and not os.path.exists(result.output_path):
                self.warnings.append(f"Output path does not exist: {result.output_path}")
                result.warnings_count += 1

            # Check data quality threshold
            if (
                self.config.enable_quality_checks
                and result.data_quality_score < self.config.quality_threshold
            ):
                warning_msg = (
                    f"Data quality score {result.data_quality_score:.3f} "
                    f"below threshold {self.config.quality_threshold}"
                )
                self.warnings.append(warning_msg)
                result.warnings_count += 1

                if self.config.enable_monitoring:
                    trigger_custom_alert(
                        title="Data Quality Warning",
                        description=warning_msg,
                        severity=AlertSeverity.MEDIUM,
                        source="etl_processor",
                    )

            # Update result with collected warnings
            result.warnings = self.warnings
            result.warnings_count = len(self.warnings)

            return result

        except Exception as e:
            self.logger.error(f"Post-processing validation failed: {e}")
            result.success = False
            result.errors = result.errors or []
            result.errors.append(f"Post-processing validation failed: {str(e)}")
            return result

    def _record_metrics(self, result: ProcessingResult):
        """Record processing metrics"""
        if not self.config.enable_monitoring or not self.metrics_collector:
            return

        try:
            processing_time = result.processing_time_seconds

            self.metrics_collector.record_etl_metrics(
                pipeline_name=f"{self.__class__.__name__}",
                stage=self.config.stage.value,
                records_processed=result.records_processed,
                records_failed=result.records_failed,
                processing_time=processing_time,
                data_quality_score=result.data_quality_score,
                error_count=result.error_count,
                warnings_count=result.warnings_count,
            )

            self.logger.info(f"Metrics recorded for {self.config.stage.value} processing")

        except Exception as e:
            self.logger.error(f"Failed to record metrics: {e}")

    def _log_completion(self, result: ProcessingResult):
        """Log processing completion details"""
        status = "SUCCESS" if result.success else "FAILED"
        self.logger.info(
            f"{self.config.stage.value.upper()} PROCESSING {status}: "
            f"Processed {result.records_processed:,} records in "
            f"{result.processing_time_seconds:.2f}s "
            f"(Quality: {result.data_quality_score:.3f})"
        )

        if result.errors:
            self.logger.error(f"Errors encountered: {len(result.errors)}")
            for error in result.errors[:5]:  # Log first 5 errors
                self.logger.error(f"  - {error}")

        if result.warnings:
            self.logger.warning(f"Warnings encountered: {len(result.warnings)}")
            for warning in result.warnings[:5]:  # Log first 5 warnings
                self.logger.warning(f"  - {warning}")

    def _create_failure_result(self, error_message: str) -> ProcessingResult:
        """Create a failure result with error details"""
        processing_time = 0.0
        if self.start_time:
            processing_time = (datetime.now() - self.start_time).total_seconds()

        return ProcessingResult(
            success=False,
            records_processed=0,
            records_failed=0,
            processing_time_seconds=processing_time,
            data_quality_score=0.0,
            error_count=1,
            warnings_count=len(self.warnings),
            errors=[error_message] + self.errors,
            warnings=self.warnings,
        )

    def save_processing_report(self, result: ProcessingResult) -> str:
        """Save detailed processing report"""
        try:
            report_dir = Path("./reports") / "etl" / self.config.stage.value
            report_dir.mkdir(parents=True, exist_ok=True)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            report_file = report_dir / f"processing_report_{timestamp}.json"

            report_data = {
                "processor": self.__class__.__name__,
                "config": {
                    "engine": self.config.engine.value,
                    "stage": self.config.stage.value,
                    "input_path": self.config.input_path,
                    "output_path": self.config.output_path,
                    "quality_threshold": self.config.quality_threshold,
                },
                "result": {
                    "success": result.success,
                    "records_processed": result.records_processed,
                    "records_failed": result.records_failed,
                    "processing_time_seconds": result.processing_time_seconds,
                    "data_quality_score": result.data_quality_score,
                    "error_count": result.error_count,
                    "warnings_count": result.warnings_count,
                    "errors": result.errors,
                    "warnings": result.warnings,
                },
                "metadata": result.metadata or {},
                "timestamp": datetime.now().isoformat(),
            }

            with open(report_file, "w") as f:
                json.dump(report_data, f, indent=2)

            self.logger.info(f"Processing report saved: {report_file}")
            return str(report_file)

        except Exception as e:
            self.logger.error(f"Failed to save processing report: {e}")
            return ""

    # Memory Management Methods

    def _get_memory_usage_mb(self) -> float:
        """Get current memory usage in MB with enhanced caching"""
        try:
            # Enhanced caching with adaptive intervals based on memory pressure
            current_time = time.time()
            
            # Determine cache interval based on memory pressure
            cache_interval = getattr(self, "_memory_cache_interval", 1.0)
            if hasattr(self, "_memory_pressure_level"):
                if self._memory_pressure_level == "critical":
                    cache_interval = 0.5  # More frequent updates under pressure
                elif self._memory_pressure_level == "high":
                    cache_interval = 0.8
                else:
                    cache_interval = 2.0  # Less frequent when memory is stable
            
            if (
                hasattr(self, "_last_memory_reading_time")
                and current_time - self._last_memory_reading_time < cache_interval
            ):
                return getattr(self, "_cached_memory_usage", 0.0)

            process = psutil.Process()
            memory_info = process.memory_info()
            memory_mb = memory_info.rss / (1024 * 1024)  # Convert bytes to MB

            # Enhanced caching with trend tracking
            if hasattr(self, "_cached_memory_usage"):
                memory_delta = memory_mb - self._cached_memory_usage
                self._memory_trend = getattr(self, "_memory_trend", 0.0) * 0.8 + memory_delta * 0.2
                
                # Adjust pressure level based on trend
                if memory_delta > 50:  # Rapid increase
                    self._memory_pressure_level = "critical"
                elif memory_delta > 20:
                    self._memory_pressure_level = "high"
                elif abs(memory_delta) < 5:
                    self._memory_pressure_level = "normal"

            # Cache the result with timestamp
            self._cached_memory_usage = memory_mb
            self._last_memory_reading_time = current_time
            self._memory_cache_interval = cache_interval

            return memory_mb
        except Exception as e:
            self.logger.warning(f"Failed to get memory usage: {e}")
            return 0.0

    def _get_memory_percent(self) -> float:
        """Get current memory usage as percentage of system memory with caching"""
        try:
            # Cache system memory readings to reduce overhead
            current_time = time.time()
            if (
                hasattr(self, "_last_system_memory_time")
                and current_time - self._last_system_memory_time < 2.0
            ):
                return getattr(self, "_cached_memory_percent", 0.0)

            memory_percent = psutil.virtual_memory().percent

            # Cache the result
            self._cached_memory_percent = memory_percent
            self._last_system_memory_time = current_time

            return memory_percent
        except Exception as e:
            self.logger.warning(f"Failed to get memory percentage: {e}")
            return 0.0

    def _check_memory_usage(self) -> bool:
        """Check memory usage and trigger alerts if necessary"""
        if not self.config.enable_memory_monitoring:
            return True

        current_time = time.time()
        if current_time - self._last_memory_check < self.config.memory_check_interval:
            return True

        self._last_memory_check = current_time
        current_memory = self._get_memory_usage_mb()
        memory_percent = self._get_memory_percent()

        # Check against configured limits
        memory_limit = self.config.memory_limit_mb
        warning_threshold = memory_limit * self.config.memory_warning_threshold

        if current_memory > memory_limit:
            self.logger.error(
                f"Memory usage ({current_memory:.1f} MB) exceeds limit ({memory_limit} MB)"
            )
            self.errors.append(
                f"Memory limit exceeded: {current_memory:.1f} MB > {memory_limit} MB"
            )

            if self.config.enable_monitoring:
                trigger_custom_alert(
                    title="ETL Memory Limit Exceeded",
                    message=f"Processor {self.__class__.__name__} exceeded memory limit",
                    severity=AlertSeverity.CRITICAL,
                    metadata={
                        "current_memory_mb": current_memory,
                        "memory_limit_mb": memory_limit,
                        "system_memory_percent": memory_percent,
                        "processor": self.__class__.__name__,
                    },
                )
            return False

        elif current_memory > warning_threshold:
            warning_msg = f"High memory usage: {current_memory:.1f} MB (>{self.config.memory_warning_threshold * 100:.0f}% of limit)"
            self.logger.warning(warning_msg)
            self.warnings.append(warning_msg)

            if self.config.enable_monitoring:
                trigger_custom_alert(
                    title="ETL High Memory Usage",
                    message=warning_msg,
                    severity=AlertSeverity.WARNING,
                    metadata={
                        "current_memory_mb": current_memory,
                        "warning_threshold_mb": warning_threshold,
                        "system_memory_percent": memory_percent,
                        "processor": self.__class__.__name__,
                    },
                )

        # Sample metrics collection to reduce overhead
        if self.metrics_collector and self._should_record_metric(current_time):
            self.metrics_collector.record_gauge(
                "etl.memory_usage_mb",
                current_memory,
                tags={
                    "processor": self.__class__.__name__,
                    "stage": self.config.stage.value,
                    "pressure": getattr(self, "_memory_pressure_level", "normal"),
                },
            )

        return True

    def _get_batch_memory_readings(self) -> dict[str, float]:
        """Get multiple memory readings in a single batch with intelligent caching"""
        try:
            current_time = time.time()
            
            # Cache batch readings for even better performance
            cache_key = "_cached_batch_memory"
            if (
                hasattr(self, cache_key)
                and hasattr(self, "_last_batch_reading_time")
                and current_time - self._last_batch_reading_time < 2.0
            ):
                return getattr(self, cache_key)
            
            process = psutil.Process()
            memory_info = process.memory_info()
            virtual_memory = psutil.virtual_memory()
            
            # Calculate additional useful metrics
            memory_mb = memory_info.rss / (1024 * 1024)
            available_mb = virtual_memory.available / (1024 * 1024)
            
            readings = {
                "memory_mb": memory_mb,
                "memory_percent": virtual_memory.percent,
                "available_mb": available_mb,
                "memory_efficiency": available_mb / (virtual_memory.total / (1024 * 1024)) * 100,
                "swap_used_mb": psutil.swap_memory().used / (1024 * 1024) if hasattr(psutil, 'swap_memory') else 0.0,
            }
            
            # Cache the results
            setattr(self, cache_key, readings)
            self._last_batch_reading_time = current_time
            
            return readings
            
        except Exception as e:
            self.logger.warning(f"Failed to get batch memory readings: {e}")
            return {"memory_mb": 0.0, "memory_percent": 0.0, "available_mb": 0.0, "memory_efficiency": 0.0, "swap_used_mb": 0.0}

    def _should_send_alert(self, alert_type: str, current_time: float) -> bool:
        """Circuit breaker pattern for alerts to prevent spam"""
        if not hasattr(self, "_alert_timestamps"):
            self._alert_timestamps = {}

        last_alert_time = self._alert_timestamps.get(alert_type, 0)
        alert_interval = (
            300 if alert_type == "memory_critical" else 600
        )  # 5 min for critical, 10 min for warnings

        if current_time - last_alert_time >= alert_interval:
            self._alert_timestamps[alert_type] = current_time
            return True
        return False

    def _should_record_metric(self, current_time: float) -> bool:
        """Intelligent sampling for metric recording"""
        if not hasattr(self, "_last_metric_time"):
            self._last_metric_time = 0

        # Sample more frequently under high memory pressure
        interval = 30  # 30 seconds default
        if hasattr(self, "_memory_pressure_level"):
            if self._memory_pressure_level == "critical":
                interval = 10
            elif self._memory_pressure_level == "high":
                interval = 20

        if current_time - self._last_metric_time >= interval:
            self._last_metric_time = current_time
            return True
        return False

    def _trigger_garbage_collection(self) -> None:
        """Trigger garbage collection to free memory"""
        if not self.config.auto_garbage_collection:
            return

        try:
            collected = gc.collect()
            if collected > 0:
                self.logger.debug(f"Garbage collection freed {collected} objects")
        except Exception as e:
            self.logger.warning(f"Garbage collection failed: {e}")

    def _optimize_memory_for_large_dataset(self, estimated_size_mb: float) -> dict[str, Any]:
        """Optimize memory settings for large datasets"""
        optimization_config = {}

        if estimated_size_mb > self.config.memory_limit_mb * 0.5:
            # Dataset is larger than 50% of memory limit
            self.logger.info(
                f"Large dataset detected ({estimated_size_mb:.1f} MB), applying memory optimizations"
            )

            # Reduce chunk size for processing
            optimized_chunk_size = max(1000, int(self.config.chunk_size_rows * 0.5))
            optimization_config["chunk_size_rows"] = optimized_chunk_size

            # Enable compression if not already enabled
            if not self.config.compression:
                optimization_config["compression"] = "gzip"

            # Force garbage collection more frequently
            optimization_config["auto_garbage_collection"] = True

            self.logger.info(f"Memory optimizations applied: {optimization_config}")

        return optimization_config

    def process_with_memory_management(self, process_func, *args, **kwargs) -> Any:
        """Wrapper for processing functions with memory management"""
        if not self.config.enable_memory_monitoring:
            return process_func(*args, **kwargs)

        # Check memory before processing
        if not self._check_memory_usage():
            raise MemoryError("Memory limit exceeded before processing")

        try:
            # Start memory monitoring
            self._memory_monitor_active = True
            initial_memory = self._get_memory_usage_mb()

            # Execute processing function
            result = process_func(*args, **kwargs)

            # Check memory after processing
            final_memory = self._get_memory_usage_mb()
            memory_delta = final_memory - initial_memory

            if memory_delta > 0:
                self.logger.debug(
                    f"Memory usage increased by {memory_delta:.1f} MB during processing"
                )

            # Trigger garbage collection if needed
            if memory_delta > 100:  # If memory increased by more than 100MB
                self._trigger_garbage_collection()

            return result

        except Exception as e:
            self.logger.error(f"Processing failed with memory management: {e}")
            raise
        finally:
            self._memory_monitor_active = False


class ProcessorFactory:
    """Factory for creating ETL processors"""

    _processors: dict[str, type[BaseETLProcessor]] = {}

    @classmethod
    def register_processor(cls, name: str, processor_class: type[BaseETLProcessor]):
        """Register a processor class"""
        cls._processors[name] = processor_class

    @classmethod
    def create_processor(cls, name: str, config: ProcessingConfig) -> BaseETLProcessor:
        """Create a processor instance"""
        if name not in cls._processors:
            raise ValueError(f"Processor '{name}' not registered")

        processor_class = cls._processors[name]
        return processor_class(config)

    @classmethod
    def list_processors(cls) -> list[str]:
        """List available processors"""
        return list(cls._processors.keys())


class ProcessingPipeline:
    """
    Chain multiple processors together in a pipeline
    """

    def __init__(self, name: str):
        self.name = name
        self.processors: list[BaseETLProcessor] = []
        self.logger = get_logger(f"Pipeline-{name}")

    def add_processor(self, processor: BaseETLProcessor):
        """Add a processor to the pipeline"""
        self.processors.append(processor)
        self.logger.info(f"Added {processor.__class__.__name__} to pipeline")

    def run(self) -> list[ProcessingResult]:
        """Execute the complete pipeline"""
        self.logger.info(f"Starting pipeline '{self.name}' with {len(self.processors)} processors")

        results = []
        for i, processor in enumerate(self.processors):
            self.logger.info(
                f"Executing processor {i + 1}/{len(self.processors)}: {processor.__class__.__name__}"
            )

            result = processor.run()
            results.append(result)

            # Stop pipeline if processor fails (unless configured otherwise)
            if not result.success:
                self.logger.error(
                    f"Pipeline stopped due to processor failure: {processor.__class__.__name__}"
                )
                break

        # Generate pipeline summary
        total_records = sum(r.records_processed for r in results)
        total_failures = sum(r.records_failed for r in results)
        total_errors = sum(r.error_count for r in results)
        avg_quality = sum(r.data_quality_score for r in results) / len(results) if results else 0

        self.logger.info(
            f"Pipeline '{self.name}' completed: "
            f"{total_records:,} records processed, "
            f"{total_failures:,} failed, "
            f"{total_errors} errors, "
            f"avg quality: {avg_quality:.3f}"
        )

        return results


class ConfigurationManager:
    """Manage ETL processor configurations"""

    def __init__(self, config_dir: str = "./config/etl"):
        self.config_dir = Path(config_dir)
        self.config_dir.mkdir(parents=True, exist_ok=True)
        self.logger = get_logger("ConfigurationManager")

    def save_config(self, name: str, config: ProcessingConfig):
        """Save a configuration"""
        config_file = self.config_dir / f"{name}.json"

        config_data = {
            "engine": config.engine.value,
            "stage": config.stage.value,
            "input_path": config.input_path,
            "output_path": config.output_path,
            "enable_monitoring": config.enable_monitoring,
            "enable_quality_checks": config.enable_quality_checks,
            "enable_caching": config.enable_caching,
            "batch_size": config.batch_size,
            "parallel_processing": config.parallel_processing,
            "compression": config.compression,
            "partition_by": config.partition_by,
            "quality_threshold": config.quality_threshold,
            "max_errors": config.max_errors,
            "custom_config": config.custom_config,
        }

        with open(config_file, "w") as f:
            json.dump(config_data, f, indent=2)

        self.logger.info(f"Configuration saved: {config_file}")

    def load_config(self, name: str) -> ProcessingConfig:
        """Load a configuration"""
        config_file = self.config_dir / f"{name}.json"

        if not config_file.exists():
            raise FileNotFoundError(f"Configuration not found: {config_file}")

        with open(config_file) as f:
            config_data = json.load(f)

        return ProcessingConfig(
            engine=ProcessingEngine(config_data["engine"]),
            stage=ProcessingStage(config_data["stage"]),
            input_path=config_data["input_path"],
            output_path=config_data["output_path"],
            enable_monitoring=config_data.get("enable_monitoring", True),
            enable_quality_checks=config_data.get("enable_quality_checks", True),
            enable_caching=config_data.get("enable_caching", False),
            batch_size=config_data.get("batch_size"),
            parallel_processing=config_data.get("parallel_processing", False),
            compression=config_data.get("compression"),
            partition_by=config_data.get("partition_by"),
            quality_threshold=config_data.get("quality_threshold", 0.95),
            max_errors=config_data.get("max_errors", 1000),
            custom_config=config_data.get("custom_config"),
        )

    def list_configs(self) -> list[str]:
        """List available configurations"""
        config_files = self.config_dir.glob("*.json")
        return [f.stem for f in config_files]


class ProcessingOrchestrator:
    """Orchestrate ETL processing workflows"""

    def __init__(self):
        self.config_manager = ConfigurationManager()
        self.logger = get_logger("ProcessingOrchestrator")

    def execute_workflow(self, workflow_config: dict[str, Any]) -> dict[str, Any]:
        """Execute a complete ETL workflow"""
        workflow_name = workflow_config.get("name", "unnamed_workflow")
        processors_config = workflow_config.get("processors", [])

        self.logger.info(f"Executing workflow: {workflow_name}")

        pipeline = ProcessingPipeline(workflow_name)

        # Create and add processors to pipeline
        for proc_config in processors_config:
            processor_name = proc_config["processor"]
            config_name = proc_config["config"]

            # Load configuration
            config = self.config_manager.load_config(config_name)

            # Override config values if specified
            if "overrides" in proc_config:
                for key, value in proc_config["overrides"].items():
                    if hasattr(config, key):
                        setattr(config, key, value)

            # Create processor
            processor = ProcessorFactory.create_processor(processor_name, config)
            pipeline.add_processor(processor)

        # Execute pipeline
        results = pipeline.run()

        # Compile workflow results
        workflow_result = {
            "workflow_name": workflow_name,
            "total_processors": len(results),
            "successful_processors": sum(1 for r in results if r.success),
            "total_records_processed": sum(r.records_processed for r in results),
            "total_records_failed": sum(r.records_failed for r in results),
            "total_errors": sum(r.error_count for r in results),
            "total_warnings": sum(r.warnings_count for r in results),
            "average_quality_score": sum(r.data_quality_score for r in results) / len(results)
            if results
            else 0,
            "total_processing_time": sum(r.processing_time_seconds for r in results),
            "processor_results": [
                {
                    "processor": proc.__class__.__name__,
                    "success": result.success,
                    "records_processed": result.records_processed,
                    "data_quality_score": result.data_quality_score,
                    "processing_time": result.processing_time_seconds,
                }
                for proc, result in zip(pipeline.processors, results, strict=False)
            ],
        }

        self.logger.info(
            f"Workflow '{workflow_name}' completed: "
            f"{workflow_result['successful_processors']}/{workflow_result['total_processors']} processors successful"
        )

        return workflow_result

    def _warm_memory_cache(self) -> None:
        \"\"\"Pre-warm memory monitoring cache with initial readings\"\"\"
        try:
            self.logger.debug(\"Warming memory monitoring cache\")
            
            # Initialize baseline readings
            self._memory_pressure_level = \"normal\"
            self._memory_trend = 0.0
            self._get_memory_usage_mb()
            self._get_memory_percent()
            self._get_batch_memory_readings()
            
            # Set conservative initial cache intervals
            self._memory_cache_interval = 1.0
            
            self.logger.debug(\"Memory cache warmed successfully\")
            
        except Exception as e:
            self.logger.warning(f\"Failed to warm memory cache: {e}\")
    
    def _optimize_memory_monitoring(self) -> None:
        \"\"\"Optimize memory monitoring based on historical patterns\"\"\"
        try:
            # Analyze memory stability over time
            if hasattr(self, \"_memory_trend\") and hasattr(self, \"_cached_memory_usage\"):
                current_memory = self._cached_memory_usage
                trend = abs(getattr(self, \"_memory_trend\", 0.0))
                
                # Adjust monitoring frequency based on stability
                if trend < 2.0:  # Very stable memory usage
                    self._memory_cache_interval = 5.0
                elif trend < 10.0:  # Moderately stable
                    self._memory_cache_interval = 2.0
                else:  # Unstable memory usage
                    self._memory_cache_interval = 0.5
                    
                self.logger.debug(f\"Optimized memory monitoring interval to {self._memory_cache_interval}s based on trend {trend:.2f}\")
                    
        except Exception as e:
            self.logger.warning(f"Failed to optimize memory monitoring: {e}")

    # Schema Evolution Methods
    def process_with_schema_evolution(
        self, 
        data: Any,
        table_name: str,
        layer: MedallionLayer,
        **kwargs
    ) -> dict[str, Any]:
        """
        Process data with schema evolution support.
        
        Args:
            data: Input data (DataFrame or similar)
            table_name: Name of the table/dataset
            layer: Medallion layer (bronze, silver, gold)
            **kwargs: Additional parameters for schema evolution
            
        Returns:
            Dict containing evolved data and evolution metadata
        """
        if not self._schema_orchestrator:
            self.logger.warning("Schema evolution not available, returning original data")
            return {'evolved_data': data, 'schema_evolved': False, 'success': True}
        
        try:
            if layer == MedallionLayer.BRONZE:
                return self._schema_orchestrator.bronze_processor.ingest_with_schema_evolution(
                    data=data,
                    table_name=table_name,
                    source_system=kwargs.get('source_system', 'unknown')
                )
            elif layer == MedallionLayer.SILVER:
                return self._schema_orchestrator.silver_processor.transform_with_schema_validation(
                    data=data,
                    table_name=table_name,
                    business_rules=kwargs.get('business_rules'),
                    quality_checks=kwargs.get('quality_checks')
                )
            elif layer == MedallionLayer.GOLD:
                return self._schema_orchestrator.gold_processor.aggregate_with_schema_control(
                    data=data,
                    table_name=table_name,
                    downstream_systems=kwargs.get('downstream_systems'),
                    require_approval=kwargs.get('require_approval', True)
                )
            else:
                raise ValueError(f"Unsupported layer: {layer}")
                
        except Exception as e:
            self.logger.error(f"Schema evolution failed for {table_name}: {e}")
            return {'evolved_data': data, 'schema_evolved': False, 'success': False, 'error': str(e)}
    
    def handle_schema_drift(
        self,
        data: Any,
        table_name: str,
        drift_threshold: float = 0.1
    ) -> dict[str, Any]:
        """
        Detect and handle schema drift in incoming data.
        
        Args:
            data: Input data to check for drift
            table_name: Name of the table/dataset
            drift_threshold: Percentage threshold for drift detection
            
        Returns:
            Dict with drift detection results and recommendations
        """
        if not self._schema_orchestrator:
            return {'drift_detected': False, 'drift_score': 0.0}
        
        try:
            return self._schema_orchestrator.bronze_processor.handle_schema_drift(
                data=data,
                table_name=table_name,
                drift_threshold=drift_threshold
            )
        except Exception as e:
            self.logger.error(f"Schema drift detection failed: {e}")
            return {'drift_detected': False, 'error': str(e)}
    
    def validate_schema_compatibility(
        self,
        table_name: str,
        new_schema: dict[str, Any],
        layer: MedallionLayer
    ) -> dict[str, Any]:
        """
        Validate if a new schema is compatible with existing schema.
        
        Args:
            table_name: Name of the table/dataset
            new_schema: New schema to validate
            layer: Medallion layer for validation rules
            
        Returns:
            Dict with compatibility results
        """
        if not self._schema_orchestrator:
            return {'compatible': True, 'warnings': ['Schema evolution not available']}
        
        try:
            return self._schema_orchestrator.schema_handler.schema_manager.validate_schema_compatibility(
                table_name=table_name,
                new_schema=new_schema,
                layer=layer
            )
        except Exception as e:
            self.logger.error(f"Schema compatibility validation failed: {e}")
            return {'compatible': False, 'errors': [str(e)]}
    
    def get_schema_health(self) -> dict[str, Any]:
        """
        Get schema health metrics for all layers.
        
        Returns:
            Dict with health metrics across medallion layers
        """
        if not self._schema_orchestrator:
            return {'error': 'Schema evolution not available'}
        
        try:
            health_metrics = {}
            for layer in [MedallionLayer.BRONZE, MedallionLayer.SILVER, MedallionLayer.GOLD]:
                health_metrics[layer.value] = self._schema_orchestrator.schema_handler.get_layer_schema_health(layer)
            
            return {
                'layers': health_metrics,
                'overall_health': self._calculate_overall_schema_health(health_metrics)
            }
        except Exception as e:
            self.logger.error(f"Failed to get schema health: {e}")
            return {'error': str(e)}
    
    def _calculate_overall_schema_health(self, layer_metrics: dict[str, Any]) -> dict[str, Any]:
        """Calculate overall schema health across all layers."""
        try:
            total_tables = sum(metrics.get('total_tables', 0) for metrics in layer_metrics.values())
            total_changes = sum(metrics.get('recent_changes', 0) for metrics in layer_metrics.values())
            total_issues = sum(metrics.get('compatibility_issues', 0) for metrics in layer_metrics.values())
            
            health_score = 1.0
            if total_tables > 0:
                # Reduce score for high change rate
                change_rate = total_changes / total_tables
                if change_rate > 0.5:
                    health_score -= 0.3
                elif change_rate > 0.2:
                    health_score -= 0.1
                
                # Reduce score for compatibility issues
                issue_rate = total_issues / total_tables
                if issue_rate > 0.1:
                    health_score -= 0.4
                elif issue_rate > 0.05:
                    health_score -= 0.2
            
            health_level = 'excellent'
            if health_score < 0.5:
                health_level = 'poor'
            elif health_score < 0.7:
                health_level = 'fair'
            elif health_score < 0.9:
                health_level = 'good'
            
            return {
                'health_score': max(0.0, health_score),
                'health_level': health_level,
                'total_tables': total_tables,
                'total_recent_changes': total_changes,
                'total_compatibility_issues': total_issues
            }
        except Exception as e:
            self.logger.error(f"Failed to calculate overall schema health: {e}")
            return {'health_score': 0.0, 'health_level': 'unknown', 'error': str(e)}
