# ADR-001: DataFrame Engine Abstraction Strategy

## Status
Accepted

## Context

Our data engineering platform needs to support multiple processing engines to handle varying data volumes, performance requirements, and deployment environments. Different stakeholders have different preferences and constraints:

- **Development Teams**: Prefer fast, lightweight engines like Polars for local development and testing
- **Production Operations**: Require distributed processing capabilities (Spark) for large-scale data
- **Data Scientists**: Need interactive capabilities and SQL compatibility (DuckDB)
- **Enterprise Requirements**: Must support multiple cloud platforms and on-premises deployments

### Current Challenges

1. **Engine Lock-in**: Existing ETL pipelines are tightly coupled to specific processing engines
2. **Development Overhead**: Each engine requires different code patterns and optimization strategies
3. **Testing Complexity**: Ensuring identical behavior across engines is difficult
4. **Operational Complexity**: Managing different engines with different configurations and deployment patterns

### Requirements

- Support for local development (small data, fast iterations)
- Production scalability (distributed processing, large datasets)
- Engine-agnostic business logic
- Consistent behavior across engines
- Performance optimization per engine
- Easy testing and validation

## Decision

We will implement a **Strategy Pattern-based engine abstraction layer** that provides:

1. **Unified DataFrame Operations Interface**: Abstract base class defining common operations
2. **Engine-Specific Implementations**: Concrete implementations for Polars, Spark, and DuckDB
3. **Automatic Engine Selection**: Smart engine selection based on data size and environment
4. **Performance Optimizations**: Engine-specific optimizations while maintaining interface consistency
5. **Comprehensive Testing**: Cross-engine parity validation

### Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│                    ETL Business Logic                       │
├─────────────────────────────────────────────────────────────┤
│              DataFrameOperations Interface                  │
├─────────────────────────────────────────────────────────────┤
│  PolarsEngine  │  SparkEngine  │  DuckDBEngine  │  Future   │
├─────────────────────────────────────────────────────────────┤
│     Polars     │   PySpark     │    DuckDB      │    ...    │
└─────────────────────────────────────────────────────────────┘
```

### Engine Selection Strategy

| Data Size | Environment | Recommended Engine | Rationale |
|-----------|-------------|-------------------|-----------|
| < 1GB | Development | Polars | Fast, memory-efficient, great for iteration |
| 1GB - 100GB | Production | Polars | Excellent single-machine performance |
| > 100GB | Production | Spark | Distributed processing capabilities |
| Any | Testing | DuckDB | SQL compatibility, deterministic results |

## Implementation Details

### Core Interface

```python
class DataFrameOperations(ABC):
    @abstractmethod
    def read_parquet(self, path: str, columns: Optional[List[str]] = None) -> Any:
        pass
    
    @abstractmethod
    def write_parquet(self, df: Any, path: str, partition_cols: Optional[List[str]] = None) -> None:
        pass
    
    @abstractmethod
    def filter(self, df: Any, condition: Any) -> Any:
        pass
    
    # ... additional operations
```

### Engine-Specific Features

#### Polars Engine
- **Strengths**: Lazy evaluation, streaming processing, memory efficiency
- **Use Cases**: Local development, single-machine production workloads
- **Optimizations**: Streaming for large datasets, predicate pushdown

#### Spark Engine  
- **Strengths**: Distributed processing, Delta Lake integration, mature ecosystem
- **Use Cases**: Large-scale production workloads, complex analytics
- **Optimizations**: Adaptive query execution, broadcast joins, partitioning

#### DuckDB Engine
- **Strengths**: SQL compatibility, analytical performance, embedded usage
- **Use Cases**: Integration testing, analytical queries, prototyping
- **Optimizations**: Columnar processing, vectorized execution

### Configuration Management

```python
@dataclass
class EngineConfig:
    engine_type: EngineType
    spark_config: Optional[Dict[str, str]] = None
    duckdb_path: Optional[str] = ':memory:'
    partition_cols: Optional[List[str]] = None
    cache_enabled: bool = False
    streaming: bool = True
    lazy_evaluation: bool = True
```

## Consequences

### Positive Consequences

1. **Engine Flexibility**: Easy to switch between engines based on requirements
2. **Development Velocity**: Fast local development with Polars, production scale with Spark
3. **Future-Proofing**: Easy to add new engines (Ray, Modin, etc.)
4. **Testing Confidence**: Comprehensive parity testing ensures consistent behavior
5. **Performance Optimization**: Engine-specific optimizations without code duplication
6. **Reduced Complexity**: Single codebase for multiple engines

### Negative Consequences

1. **Abstraction Overhead**: Some engine-specific features may be harder to access
2. **Complexity**: Additional abstraction layer adds some complexity
3. **Performance**: Minor performance overhead from abstraction (negligible in practice)
4. **Maintenance**: Need to maintain multiple engine implementations

### Risk Mitigation

1. **Feature Parity**: Comprehensive test suite validates equivalent behavior across engines
2. **Performance**: Engine-specific optimizations ensure minimal performance impact
3. **Maintenance**: Clear separation of concerns and comprehensive documentation
4. **Escape Hatch**: Direct engine access available when needed for advanced features

## Alternatives Considered

### Alternative 1: Single Engine Approach
**Decision**: Use only Spark for all workloads
**Rejected Because**: 
- Poor development experience (slow startup, complex setup)
- Resource overhead for small datasets
- Vendor lock-in concerns

### Alternative 2: Separate Codebases
**Decision**: Maintain separate ETL pipelines for each engine
**Rejected Because**:
- Code duplication and maintenance burden
- Inconsistent business logic across engines
- Testing complexity increases exponentially

### Alternative 3: Adapter Pattern
**Decision**: Create adapters for each engine without unified interface
**Rejected Because**:
- Doesn't solve the interface consistency problem
- Still requires engine-specific code in business logic
- Limited abstraction benefits

## Implementation Timeline

### Phase 1: Core Infrastructure ✅
- [x] Define DataFrameOperations interface
- [x] Implement PolarsEngine with core operations
- [x] Implement SparkEngine with distributed optimizations
- [x] Create EngineFactory with auto-selection
- [x] Basic configuration management

### Phase 2: Advanced Features
- [ ] DuckDB engine implementation
- [ ] Delta Lake integration for Spark
- [ ] Advanced SCD Type 2 operations
- [ ] Custom partitioning strategies
- [ ] Memory management optimizations

### Phase 3: Production Hardening
- [ ] Comprehensive error handling
- [ ] Performance benchmarking suite
- [ ] Production deployment patterns
- [ ] Monitoring and observability integration

## Success Metrics

1. **Development Velocity**: 50% reduction in local development iteration time
2. **Code Reuse**: 90%+ code reuse across engines for business logic
3. **Performance**: <5% performance overhead from abstraction layer
4. **Test Coverage**: 100% parity test coverage for core operations
5. **Operational Excellence**: Zero engine-related production incidents

## Related Decisions

- [ADR-002: Star Schema Grain Strategy](ADR-002-star-schema-grain.md)
- [ADR-003: Data Quality Framework](ADR-003-data-quality-framework.md) (planned)
- [ADR-004: Observability Strategy](ADR-004-observability-strategy.md) (planned)

## References

- [Polars Documentation](https://pola-rs.github.io/polars/)
- [Apache Spark Documentation](https://spark.apache.org/docs/latest/)
- [DuckDB Documentation](https://duckdb.org/docs/)
- [Strategy Pattern - Design Patterns](https://refactoring.guru/design-patterns/strategy)
- [Data Engineering Design Patterns](https://www.oreilly.com/library/view/data-engineering-design/9781098157326/)