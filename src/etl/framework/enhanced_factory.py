"""
Enhanced Factory Pattern for ETL Processors
Provides improved type safety, plugin architecture, and configuration management.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Type, TypeVar, Generic, Protocol
from dataclasses import dataclass, field
from enum import Enum
import importlib
import inspect
import logging
from pathlib import Path

from .base_processor import BaseETLProcessor, ProcessingConfig, ProcessingEngine, ProcessingStage
from core.logging import get_logger


T = TypeVar('T', bound=BaseETLProcessor)


class ProcessorCapability(Enum):
    """Capabilities that processors can support"""
    STREAMING = "streaming"
    BATCH = "batch"
    REAL_TIME = "real_time"
    DISTRIBUTED = "distributed"
    CACHING = "caching"
    COMPRESSION = "compression"
    ENCRYPTION = "encryption"
    VALIDATION = "validation"
    MONITORING = "monitoring"
    RETRY_LOGIC = "retry_logic"


@dataclass
class ProcessorMetadata:
    """Metadata about a processor implementation"""
    name: str
    version: str
    description: str
    engine: ProcessingEngine
    supported_stages: List[ProcessingStage]
    capabilities: List[ProcessorCapability] = field(default_factory=list)
    dependencies: List[str] = field(default_factory=list)
    config_schema: Optional[Dict[str, Any]] = None
    performance_profile: Optional[Dict[str, Any]] = None
    author: Optional[str] = None
    license: Optional[str] = None


class ProcessorPlugin(Protocol):
    """Protocol for processor plugins"""
    
    @property
    def metadata(self) -> ProcessorMetadata:
        """Return processor metadata"""
        ...
    
    def create_processor(self, config: ProcessingConfig) -> BaseETLProcessor:
        """Create a processor instance with given configuration"""
        ...
    
    def validate_config(self, config: ProcessingConfig) -> bool:
        """Validate the configuration for this processor"""
        ...


@dataclass
class ProcessorRegistration:
    """Registration information for a processor"""
    processor_class: Type[BaseETLProcessor]
    metadata: ProcessorMetadata
    plugin: Optional[ProcessorPlugin] = None
    is_active: bool = True


class ProcessorRegistry:
    """Central registry for all ETL processors"""
    
    def __init__(self):
        self._processors: Dict[str, ProcessorRegistration] = {}
        self._by_engine: Dict[ProcessingEngine, List[str]] = {}
        self._by_stage: Dict[ProcessingStage, List[str]] = {}
        self._by_capability: Dict[ProcessorCapability, List[str]] = {}
        self.logger = get_logger(self.__class__.__name__)
    
    def register(
        self, 
        name: str, 
        processor_class: Type[BaseETLProcessor],
        metadata: ProcessorMetadata,
        plugin: Optional[ProcessorPlugin] = None,
        override: bool = False
    ) -> None:
        """Register a processor with the registry"""
        
        if name in self._processors and not override:
            raise ValueError(f"Processor '{name}' already registered. Use override=True to replace.")
        
        # Validate processor class
        if not issubclass(processor_class, BaseETLProcessor):
            raise TypeError(f"Processor class must inherit from BaseETLProcessor")
        
        registration = ProcessorRegistration(
            processor_class=processor_class,
            metadata=metadata,
            plugin=plugin
        )
        
        self._processors[name] = registration
        
        # Update indexes
        self._update_indexes(name, metadata)
        
        self.logger.info(f"Registered processor: {name} ({metadata.engine.value})")
    
    def _update_indexes(self, name: str, metadata: ProcessorMetadata) -> None:
        """Update internal indexes for fast lookup"""
        
        # Engine index
        if metadata.engine not in self._by_engine:
            self._by_engine[metadata.engine] = []
        if name not in self._by_engine[metadata.engine]:
            self._by_engine[metadata.engine].append(name)
        
        # Stage index
        for stage in metadata.supported_stages:
            if stage not in self._by_stage:
                self._by_stage[stage] = []
            if name not in self._by_stage[stage]:
                self._by_stage[stage].append(name)
        
        # Capability index
        for capability in metadata.capabilities:
            if capability not in self._by_capability:
                self._by_capability[capability] = []
            if name not in self._by_capability[capability]:
                self._by_capability[capability].append(name)
    
    def unregister(self, name: str) -> None:
        """Unregister a processor"""
        if name not in self._processors:
            raise KeyError(f"Processor '{name}' not found")
        
        registration = self._processors[name]
        metadata = registration.metadata
        
        # Remove from indexes
        self._by_engine[metadata.engine].remove(name)
        for stage in metadata.supported_stages:
            self._by_stage[stage].remove(name)
        for capability in metadata.capabilities:
            self._by_capability[capability].remove(name)
        
        del self._processors[name]
        self.logger.info(f"Unregistered processor: {name}")
    
    def get_registration(self, name: str) -> ProcessorRegistration:
        """Get processor registration by name"""
        if name not in self._processors:
            raise KeyError(f"Processor '{name}' not found")
        return self._processors[name]
    
    def list_processors(
        self,
        engine: Optional[ProcessingEngine] = None,
        stage: Optional[ProcessingStage] = None,
        capability: Optional[ProcessorCapability] = None,
        active_only: bool = True
    ) -> List[str]:
        """List processors matching criteria"""
        
        candidates = set(self._processors.keys())
        
        if engine:
            candidates &= set(self._by_engine.get(engine, []))
        
        if stage:
            candidates &= set(self._by_stage.get(stage, []))
        
        if capability:
            candidates &= set(self._by_capability.get(capability, []))
        
        if active_only:
            candidates = {name for name in candidates if self._processors[name].is_active}
        
        return sorted(list(candidates))
    
    def get_metadata(self, name: str) -> ProcessorMetadata:
        """Get processor metadata"""
        return self.get_registration(name).metadata
    
    def get_best_processor(
        self,
        config: ProcessingConfig,
        required_capabilities: Optional[List[ProcessorCapability]] = None
    ) -> str:
        """Get the best processor for given configuration and requirements"""
        
        candidates = self.list_processors(
            engine=config.engine,
            stage=config.stage
        )
        
        if required_capabilities:
            for capability in required_capabilities:
                capability_processors = set(self._by_capability.get(capability, []))
                candidates = [name for name in candidates if name in capability_processors]
        
        if not candidates:
            raise ValueError(f"No suitable processor found for {config.engine.value}/{config.stage.value}")
        
        # Simple scoring based on capabilities match
        scored_candidates = []
        for name in candidates:
            metadata = self.get_metadata(name)
            score = len(metadata.capabilities)
            if required_capabilities:
                matching_caps = len(set(required_capabilities) & set(metadata.capabilities))
                score += matching_caps * 10
            scored_candidates.append((score, name))
        
        # Return highest scoring processor
        scored_candidates.sort(reverse=True)
        return scored_candidates[0][1]


class EnhancedProcessorFactory:
    """Enhanced factory with improved capabilities"""
    
    def __init__(self, registry: Optional[ProcessorRegistry] = None):
        self.registry = registry or ProcessorRegistry()
        self.logger = get_logger(self.__class__.__name__)
        self._config_validators: Dict[str, callable] = {}
        
        # Auto-discover and register processors
        self._auto_discover_processors()
    
    def register_processor(
        self,
        name: str,
        processor_class: Type[BaseETLProcessor],
        metadata: ProcessorMetadata,
        plugin: Optional[ProcessorPlugin] = None,
        config_validator: Optional[callable] = None
    ) -> None:
        """Register a processor with optional config validator"""
        
        self.registry.register(name, processor_class, metadata, plugin)
        
        if config_validator:
            self._config_validators[name] = config_validator
    
    def create_processor(
        self,
        name: str,
        config: ProcessingConfig,
        validate_config: bool = True
    ) -> BaseETLProcessor:
        """Create a processor instance with enhanced validation"""
        
        try:
            registration = self.registry.get_registration(name)
            
            if validate_config:
                self._validate_configuration(name, config, registration)
            
            # Create processor instance
            if registration.plugin:
                processor = registration.plugin.create_processor(config)
            else:
                processor = registration.processor_class(config)
            
            self.logger.info(f"Created processor: {name}")
            return processor
            
        except Exception as e:
            self.logger.error(f"Failed to create processor '{name}': {e}")
            raise
    
    def create_best_processor(
        self,
        config: ProcessingConfig,
        required_capabilities: Optional[List[ProcessorCapability]] = None
    ) -> BaseETLProcessor:
        """Create the best processor for given requirements"""
        
        best_name = self.registry.get_best_processor(config, required_capabilities)
        return self.create_processor(best_name, config)
    
    def _validate_configuration(
        self,
        name: str,
        config: ProcessingConfig,
        registration: ProcessorRegistration
    ) -> None:
        """Validate processor configuration"""
        
        # Basic validation
        metadata = registration.metadata
        
        if config.engine != metadata.engine:
            raise ValueError(f"Engine mismatch: processor expects {metadata.engine.value}, got {config.engine.value}")
        
        if config.stage not in metadata.supported_stages:
            raise ValueError(f"Stage not supported: {config.stage.value}")
        
        # Custom validator
        if name in self._config_validators:
            validator = self._config_validators[name]
            if not validator(config):
                raise ValueError(f"Configuration validation failed for processor '{name}'")
        
        # Plugin validation
        if registration.plugin:
            if not registration.plugin.validate_config(config):
                raise ValueError(f"Plugin configuration validation failed for processor '{name}'")
    
    def _auto_discover_processors(self) -> None:
        """Auto-discover processors from standard locations"""
        
        try:
            # Import and register built-in processors
            self._register_builtin_processors()
            
            # Scan for additional processors
            self._scan_processor_modules()
            
        except Exception as e:
            self.logger.warning(f"Error during auto-discovery: {e}")
    
    def _register_builtin_processors(self) -> None:
        """Register built-in processors"""
        
        try:
            # Import pandas processors
            from etl.bronze.pandas_bronze import PandasBronzeProcessor
            self.registry.register(
                "pandas_bronze",
                PandasBronzeProcessor,
                ProcessorMetadata(
                    name="pandas_bronze",
                    version="1.0.0",
                    description="Pandas-based bronze layer processor",
                    engine=ProcessingEngine.PANDAS,
                    supported_stages=[ProcessingStage.BRONZE],
                    capabilities=[
                        ProcessorCapability.BATCH,
                        ProcessorCapability.VALIDATION,
                        ProcessorCapability.MONITORING
                    ]
                )
            )
            
            from etl.silver.pandas_silver import PandasSilverProcessor
            self.registry.register(
                "pandas_silver",
                PandasSilverProcessor,
                ProcessorMetadata(
                    name="pandas_silver",
                    version="1.0.0",
                    description="Pandas-based silver layer processor",
                    engine=ProcessingEngine.PANDAS,
                    supported_stages=[ProcessingStage.SILVER],
                    capabilities=[
                        ProcessorCapability.BATCH,
                        ProcessorCapability.VALIDATION,
                        ProcessorCapability.MONITORING
                    ]
                )
            )
            
            # Import Spark processors
            from etl.spark.enhanced_processors import (
                EnhancedBronzeProcessor, 
                EnhancedSilverProcessor, 
                EnhancedGoldProcessor
            )
            
            self.registry.register(
                "spark_bronze",
                EnhancedBronzeProcessor,
                ProcessorMetadata(
                    name="spark_bronze",
                    version="1.0.0",
                    description="Spark-based bronze layer processor",
                    engine=ProcessingEngine.SPARK,
                    supported_stages=[ProcessingStage.BRONZE],
                    capabilities=[
                        ProcessorCapability.BATCH,
                        ProcessorCapability.DISTRIBUTED,
                        ProcessorCapability.VALIDATION,
                        ProcessorCapability.MONITORING,
                        ProcessorCapability.COMPRESSION
                    ]
                )
            )
            
            self.registry.register(
                "spark_silver",
                EnhancedSilverProcessor,
                ProcessorMetadata(
                    name="spark_silver",
                    version="1.0.0",
                    description="Spark-based silver layer processor",
                    engine=ProcessingEngine.SPARK,
                    supported_stages=[ProcessingStage.SILVER],
                    capabilities=[
                        ProcessorCapability.BATCH,
                        ProcessorCapability.DISTRIBUTED,
                        ProcessorCapability.VALIDATION,
                        ProcessorCapability.MONITORING,
                        ProcessorCapability.COMPRESSION
                    ]
                )
            )
            
            self.registry.register(
                "spark_gold",
                EnhancedGoldProcessor,
                ProcessorMetadata(
                    name="spark_gold",
                    version="1.0.0",
                    description="Spark-based gold layer processor",
                    engine=ProcessingEngine.SPARK,
                    supported_stages=[ProcessingStage.GOLD],
                    capabilities=[
                        ProcessorCapability.BATCH,
                        ProcessorCapability.DISTRIBUTED,
                        ProcessorCapability.VALIDATION,
                        ProcessorCapability.MONITORING,
                        ProcessorCapability.COMPRESSION
                    ]
                )
            )
            
        except ImportError as e:
            self.logger.warning(f"Could not register some built-in processors: {e}")
    
    def _scan_processor_modules(self) -> None:
        """Scan for processor modules in standard locations"""
        
        # This could be extended to scan for plugins in specific directories
        pass
    
    def get_processor_info(self, name: str) -> Dict[str, Any]:
        """Get comprehensive information about a processor"""
        
        registration = self.registry.get_registration(name)
        metadata = registration.metadata
        
        return {
            "name": metadata.name,
            "version": metadata.version,
            "description": metadata.description,
            "engine": metadata.engine.value,
            "supported_stages": [stage.value for stage in metadata.supported_stages],
            "capabilities": [cap.value for cap in metadata.capabilities],
            "dependencies": metadata.dependencies,
            "is_active": registration.is_active,
            "has_plugin": registration.plugin is not None,
            "class_name": registration.processor_class.__name__,
            "module": registration.processor_class.__module__
        }
    
    def list_processors_detailed(self) -> List[Dict[str, Any]]:
        """Get detailed information about all processors"""
        
        processors = self.registry.list_processors(active_only=False)
        return [self.get_processor_info(name) for name in processors]


# Global factory instance
_global_factory: Optional[EnhancedProcessorFactory] = None


def get_processor_factory() -> EnhancedProcessorFactory:
    """Get the global processor factory instance"""
    global _global_factory
    if _global_factory is None:
        _global_factory = EnhancedProcessorFactory()
    return _global_factory


def create_processor(
    name: str,
    config: ProcessingConfig,
    validate_config: bool = True
) -> BaseETLProcessor:
    """Convenience function to create a processor"""
    factory = get_processor_factory()
    return factory.create_processor(name, config, validate_config)


def create_best_processor(
    config: ProcessingConfig,
    required_capabilities: Optional[List[ProcessorCapability]] = None
) -> BaseETLProcessor:
    """Convenience function to create the best processor for requirements"""
    factory = get_processor_factory()
    return factory.create_best_processor(config, required_capabilities)