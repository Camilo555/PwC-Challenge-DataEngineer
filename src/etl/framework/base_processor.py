"""
Base ETL Processor Framework
Provides a unified interface and common functionality for all ETL processors.
"""

import json
import os
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

from core.logging import get_logger
from monitoring import AlertSeverity, get_metrics_collector, trigger_custom_alert


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

        # Initialize processing state
        self._initialize_processor()

    def _initialize_processor(self):
        """Initialize processor-specific components"""
        self.logger.info(f"Initializing {self.__class__.__name__} with {self.config.engine.value} engine")

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
                    source="etl_processor"
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
            if (self.config.enable_quality_checks and
                result.data_quality_score < self.config.quality_threshold):
                warning_msg = (f"Data quality score {result.data_quality_score:.3f} "
                             f"below threshold {self.config.quality_threshold}")
                self.warnings.append(warning_msg)
                result.warnings_count += 1

                if self.config.enable_monitoring:
                    trigger_custom_alert(
                        title="Data Quality Warning",
                        description=warning_msg,
                        severity=AlertSeverity.MEDIUM,
                        source="etl_processor"
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
                warnings_count=result.warnings_count
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
            warnings=self.warnings
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
                    "quality_threshold": self.config.quality_threshold
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
                    "warnings": result.warnings
                },
                "metadata": result.metadata or {},
                "timestamp": datetime.now().isoformat()
            }

            with open(report_file, 'w') as f:
                json.dump(report_data, f, indent=2)

            self.logger.info(f"Processing report saved: {report_file}")
            return str(report_file)

        except Exception as e:
            self.logger.error(f"Failed to save processing report: {e}")
            return ""


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
            self.logger.info(f"Executing processor {i+1}/{len(self.processors)}: {processor.__class__.__name__}")

            result = processor.run()
            results.append(result)

            # Stop pipeline if processor fails (unless configured otherwise)
            if not result.success:
                self.logger.error(f"Pipeline stopped due to processor failure: {processor.__class__.__name__}")
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
            "custom_config": config.custom_config
        }

        with open(config_file, 'w') as f:
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
            custom_config=config_data.get("custom_config")
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
            "average_quality_score": sum(r.data_quality_score for r in results) / len(results) if results else 0,
            "total_processing_time": sum(r.processing_time_seconds for r in results),
            "processor_results": [
                {
                    "processor": proc.__class__.__name__,
                    "success": result.success,
                    "records_processed": result.records_processed,
                    "data_quality_score": result.data_quality_score,
                    "processing_time": result.processing_time_seconds
                }
                for proc, result in zip(pipeline.processors, results, strict=False)
            ]
        }

        self.logger.info(
            f"Workflow '{workflow_name}' completed: "
            f"{workflow_result['successful_processors']}/{workflow_result['total_processors']} processors successful"
        )

        return workflow_result
