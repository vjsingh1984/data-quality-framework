# Data Quality Framework - Roadmap Tracker

**Last Updated**: 2026-02-07
**Status**: Tracking remaining design/architecture improvements and vision items
**Completed**: All critical, high, medium, and low-priority implementation shortcomings (Phases A-D)

---

## High-Level Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           DQ Framework Architecture                           │
└─────────────────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                            Configuration Layer                               │
│  ┌──────────────┐    ┌──────────────┐    ┌──────────────┐                   │
│  │  HOCON File  │───▶│ ConfigLoader │───▶│ ConfigTree   │                   │
│  │  / S3 / ADLS │    │              │    │  (pyhocon)   │                   │
│  └──────────────┘    └──────────────┘    └──────────────┘                   │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                          DQFramework Orchestrator                            │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │  _load_dataframes()                                                  │    │
│  │    ├── CatalogFactory ──▶ CatalogProvider ──▶ DataFrame              │    │
│  │    └── ChainedResolver (default → config → spark → catalog)          │    │
│  │                                                                      │    │
│  │  run()                                                               │    │
│  │    └── For each rule:                                                │    │
│  │        ├── EngineLoader.load_engine(name)                           │    │
│  │        ├── engine.apply(dataframe) ──▶ List[Dict]                   │    │
│  │        └── Accumulate metrics                                         │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                  ┌───────────────────┼───────────────────┐
                  │                   │                   │
                  ▼                   ▼                   ▼
┌─────────────────────────────┐  ┌────────────────────────────────┐  ┌──────────────────┐
│      Engine Registries       │  │      Catalog Providers         │  │  Resolvers       │
│  ┌─────────────────────┐     │  │  ┌─────────────────────────┐  │  │  ┌────────────┐  │
│  │   EngineRegistry    │     │  │  │  SparkCatalogProvider    │  │  │  │ DefaultDF   │  │
│  │   ┌───────────────┐ │     │  │  │  UnityCatalogProvider    │  │  │  │ ConfigDF    │  │
│  │   │deequ          │ │     │  │  │  GlueCatalogProvider     │  │  │  │ SparkCatalog│  │
│  │   │custom         │ │     │  │  └─────────────────────────┘  │  │  │ Catalog     │  │
│  │   │schemavalidation││     │  └────────────────────────────────┘  │  │  Provider    │  │
│  │   │greatexpect.   │ │     │                                      │  │  └────────────┘  │
│  │   │drules         │ │     │  ┌────────────────────────────────┐  │  │  (Chain)       │
│  │   └───────────────┘ │     │  │      Constraint Registry        │  │  └──────────────────┘
│  └─────────────────────┘     │  │  ┌─────────────────────────┐    │  │
│                             │  │  │ RateOfChange            │    │  │
│  ┌─────────────────────┐     │  │  │ DistinctnessByGroup     │    │  │
│  │   RuleRegistry      │     │  │  │ LookupBasedOnColList    │    │  │
│  │   ┌───────────────┐ │     │  │  │ NegativeValuesCheck     │    │  │
│  │   │column_threshold│ │     │  │  └─────────────────────────┘    │  │
│  │   │cross_column   │ │     │  └────────────────────────────────┘  │
│  │   │custom_sql     │ │     │                                      │
│  │   │null_check     │ │     │  ┌────────────────────────────────┐  │
│  │   │regex          │ │     │  │      Rule Registry              │  │
│  │   └───────────────┘ │     │  │  ┌─────────────────────────┐    │  │
│  └─────────────────────┘     │  │  │ ColumnThresholdRule     │    │  │
│                             │  │  │ CrossColumnRule         │    │  │
│                             │  │  │ CustomSQLRule           │    │  │
│                             │  │  │ NullCheckRule           │    │  │
│                             │  │  │ RegexRule               │    │  │
│                             │  │  └─────────────────────────┘    │  │
│                             │  └────────────────────────────────┘  │
│                             └─────────────────────────────────────┘
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Output Metrics Layer                               │
│  List[Dict {                                                              │
│    "check": str,              # Constraint name                             │
│    "success": bool,           # Pass/fail                                   │
│    "details": Dict,           # Engine-specific details                      │
│    "ts": int,                 # Timestamp (ms)                              │
│    "jobid": str               # Spark application ID                        │
│  }]                                                                       │
└─────────────────────────────────────────────────────────────────────────────┘

                              ┌─────────────────────────┐
                              │   Remaining Issues      │
                              ├─────────────────────────┤
                              │ D3: Registry pattern    │
                              │ D4: SchemaVal-Deequ     │
                              │ V1-V6: Vision items     │
                              └─────────────────────────┘
```

---

## Completed Work (2026-02-07)

### Commits Pushed
1. `2f10bc2` - Fix critical framework shortcomings and improve code quality
2. `56382ae` - Add JSON serialization fallback for failed check logging
3. `e3acb72` - Add roadmap tracker documentation

### Summary of Completed Items

All implementation shortcomings (I1-I22) and API improvements (I16, D1) have been addressed:
- ✓ Thread-safe registries with `threading.Lock`
- ✓ Global cache replaced with `@functools.lru_cache`
- ✓ Error handling improvements (re-raise instead of silent swallowing)
- ✓ Defensive coding for IndexError and empty results
- ✓ PEP 8 naming consistency (camelCase → snake_case)
- ✓ Deprecation warnings for `repository` parameter
- ✓ Lazy imports for optional dependencies (boto3, pydeequ)
- ✓ SQL injection prevention (Unity Catalog, Drules)
- ✓ JSON serialization fallbacks

**Test Status**: 135/135 unit tests passing ✓

---

## Remaining Design & Architecture Issues

### D2: Return Type Variance Across Engines

**ID**: D2 / I20
**Priority**: Medium
**Category**: Design
**Status**: ✅ **COMPLETED** (2026-02-07)

#### Description
Different engines returned metrics with inconsistent structure. This made it difficult for consumers to process results uniformly.

#### Current State Examples

```python
# DeequEngine returns:
{
    "check": "Check name",
    "success": True/False,
    "details": {...}  # Spark Row with constraint_status, check, etc.
}

# DrulesEngine returns:
{
    "check": "constraint_name",
    "success": True/False,
    "details": {"error": "..."} or {...}
}

# GreatExpectationsEngine returns:
{
    "check": "expect_column_values_to_not_be_null",
    "success": True/False,
    "details": {"observed_value": 42, "element_count": 1000}
}
```

#### Visual Representation of Problem

```
┌─────────────────────────────────────────────────────────────────┐
│                     DQFramework.run()                          │
└─────────────────────────────────────────────────────────────────┘
                              │
            ┌─────────────────┼─────────────────┐
            │                 │                 │
    ┌───────▼──────┐  ┌───────▼──────┐  ┌──────▼─────┐
    │  DeequEngine │  │  DrulesEngine│  │    GE...   │
    └───────┬──────┘  └───────┬──────┘  └──────┬─────┘
            │                 │                 │
            │ INCONSISTENT    │ INCONSISTENT    │ INCONSISTENT
            ▼                 ▼                 ▼
    {check,             {constraint_name,  {expectation_type,
     success,            success,           success,
     details:{...}}     details:{...}}    details:{...}}
            │                 │                 │
            └─────────────────┴─────────────────┘
                              │
                    ┌─────────▼─────────┐
                    │  Consumer Code    │
                    │  ❌ Can't handle  │
                    │  multiple formats│
                    └───────────────────┘
```

#### Target State with Normalized Schema

```
┌─────────────────────────────────────────────────────────────────┐
│                     DQFramework.run()                          │
└─────────────────────────────────────────────────────────────────┘
                              │
            ┌─────────────────┼─────────────────┐
            │                 │                 │
    ┌───────▼──────┐  ┌───────▼──────┐  ┌──────▼─────┐
    │  DeequEngine │  │  DrulesEngine│  │    GE...   │
    │  +normalize()│  │  +normalize()│  │+normalize()│
    └───────┬──────┘  └───────┬──────┘  └──────┬─────┘
            │                 │                 │
            │ all return      │ all return      │ all return
            ▼                 ▼                 ▼
    ┌─────────────────────────────────────────────────┐
    │              DQMetric (dataclass)               │
    │  • check: str                                   │
    │  • constraint: str                              │
    │  • success: bool                                │
    │  • engine: str                                  │
    │  • timestamp_ms: int                            │
    │  • dataset: str                                 │
    │  • details: Dict[str, Any]                      │
    │  • execution_time_ms: Optional[int]             │
    │  • assertion: Optional[str]                     │
    └─────────────────────────────────────────────────┘
            │                 │                 │
            └─────────────────┴─────────────────┘
                              │
                    ┌─────────▼─────────┐
                    │  Consumer Code    │
                    │  ✓ Single format  │
                    │  ✓ Predictable    │
                    └───────────────────┘
```

#### Impact
- Consumers must handle multiple metric formats
- Difficult to build unified dashboards or alerting systems
- Inconsistent field names (`constraint_status` vs `success`)

#### Recommended Solution

1. **Define normalized metric schema**:
```python
@dataclass
class DQMetric:
    check: str
    constraint: str
    success: bool
    engine: str
    timestamp_ms: int
    dataset: str
    details: Dict[str, Any]
    execution_time_ms: Optional[int] = None
    assertion: Optional[str] = None
```

2. **Add adapter layer in each engine**:
```python
class DeequEngine(DQEngine):
    def apply(self, dataframe, repository=None) -> List[DQMetric]:
        # ... existing logic ...
        return [self._normalize_metric(m) for m in raw_metrics]

    def _normalize_metric(self, raw: Dict) -> DQMetric:
        return DQMetric(
            check=raw["check"],
            constraint=raw["check"].split(" - ")[0],
            success=raw["success"],
            engine="deequ",
            timestamp_ms=raw.get("ts"),
            dataset=self._config.get("dataset"),
            details=raw.get("details", {}),
            execution_time_ms=raw.get("execution_time")
        )
```

3. **Update `DQEngine.run()` to enforce schema**:
```python
# In dq_framework.py
for metric in metrics:
    if not isinstance(metric, DQMetric):
        raise TypeError(f"Engine must return DQMetric, got {type(metric)}")
```

#### Files to Modify
- `dq/engine/dq_engine.py` - Add `DQMetric` dataclass
- `dq/engine/deequ/deequ_engine.py` - Add normalization
- `dq/engine/custom/custom_engine.py` - Add normalization
- `dq/engine/schemavalidation/schemavalidation_engine.py` - Add normalization
- `dq/engine/drules/drules_engine.py` - Add normalization
- `dq/engine/greatexpectations/greatexpectations_engine.py` - Add normalization

#### Implementation Details

**Added DQMetric dataclass:**
```python
@dataclass
class DQMetric:
    """Normalized metric returned by all data quality engines."""
    check: str
    constraint: str
    success: bool
    engine: str
    timestamp_ms: int
    dataset: str
    details: Dict[str, Any] = field(default_factory=dict)
    execution_time_ms: Optional[int] = None
    assertion: Optional[str] = None

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for backward compatibility."""
        return asdict(self)

    @classmethod
    def time_ms(cls) -> int:
        """Get current time in milliseconds since epoch."""
        import time
        return int(time.time() * 1000)
```

**Added helper methods to DQEngine:**
```python
def _get_engine_name(self) -> str:
    """Get the engine name for metrics."""

def _get_dataset_name(self) -> str:
    """Get the dataset name from config."""

def _create_metric(self, check, success, details, constraint=None,
                   execution_time_ms=None, assertion=None) -> DQMetric:
    """Create a normalized metric with common fields populated."""
```

**Updated all engines:**
- Engines call `self._create_metric()` to create metrics with normalized fields
- Convert to dict via `metric.to_dict()` for backward compatibility
- Extract constraint from check name where needed

**Files Modified:**
- `dq/engine/dq_engine.py` - Added DQMetric dataclass and helper methods
- `dq/engine/deequ/deequ_engine.py` - Use DQMetric for metrics
- `dq/engine/custom/custom_engine.py` - Use DQMetric for metrics
- `dq/engine/schemavalidation/schemavalidation_engine.py` - Use DQMetric for metrics
- `dq/engine/drules/drules_engine.py` - Use DQMetric for metrics
- `dq/engine/greatexpectations/greatexpectations_engine.py` - Use DQMetric for metrics
- `dq/utils/constants.py` - Added DQ_DATASET constant
- `dq/tests/unit/test_greatexpectations_unit.py` - Fixed tests to mock config

#### Test Results
All 173 unit tests pass

#### Commit
`c1042fc` - Implement D2: Return type variance with normalized DQMetric

#### Estimated Effort
2-3 days

#### Dependencies
- None

**Unblocks**: D4 (SchemaValidation-Deequ decoupling)

---

### D3: Dual Registry Pattern Inconsistency

**ID**: D3
**Priority**: Low
**Category**: Design
**Status**: **OPEN**

#### Description
Three registries exist with different patterns:
- `EngineRegistry` - Uses `_lock` for thread safety, has `get_engine_class()`, `unregister()`
- `ConstraintRegistry` - Uses `_lock`, but has `get()` instead of `get_constraint_class()`
- `RuleRegistry` - Uses `_lock`, has `get()` instead of `get_rule_class()`

#### Current State
```python
# EngineRegistry
cls._explicit[name.lower()] = engine_class
engine_class = EngineRegistry.get_engine_class(name)  # Returns class

# ConstraintRegistry
cls._constraints[name] = constraint_class
constraint_class = ConstraintRegistry.get(name)  # Returns class

# RuleRegistry
cls._rules[name] = rule_class
rule_class = RuleRegistry.get(name)  # Returns class
```

#### Visual Representation of Current Inconsistency

```
┌──────────────────────────────────────────────────────────────┐
│                    Current State (INCONSISTENT)               │
├──────────────────────────────────────────────────────────────┤
│                                                               │
│  EngineRegistry                          ConstraintRegistry    │
│  ┌─────────────────┐                   ┌─────────────────┐  │
│  │ _explicit dict  │                   │ _constraints    │  │
│  │ _lock           │                   │ _lock           │  │
│  │ register()      │                   │ register()      │  │
│  │ get_engine_cls()│  ─── DIFF ────►   │ get()           │  │
│  │ unregister()    │                   │ NO unregister   │  │
│  └─────────────────┘                   └─────────────────┘  │
│         ▼                                         ▲          │
│         │                                         │          │
│         └───────────┬─────────────────────────────┘          │
│                     │                                        │
│  RuleRegistry                                          │
│  ┌─────────────────┐                   ┌─────────────────┐  │
│  │ _rules dict     │                   │  ???            │  │
│  │ _lock           │    ─── DIFF ────►  Confusing API   │  │
│  │ register()      │                   │ surface         │  │
│  │ get()           │                                   │  │
│  │ unregister()    │                                   │  │
│  └─────────────────┘                                   │  │
└──────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────┐
│                   Target State (CONSISTENT)                  │
├──────────────────────────────────────────────────────────────┤
│                                                               │
│                    GenericRegistry[T]                         │
│                    ┌─────────────────┐                       │
│                    │ _registry dict  │                       │
│                    │ _lock           │                       │
│                    │ register()      │◄────── All inherit    │
│                    │ get()           │        from this       │
│                    │ unregister()    │                       │
│                    │ list_items()    │                       │
│                    └─────────────────┘                       │
│                           │                                  │
│         ┌─────────────────┼─────────────────┐                │
│         │                 │                 │                │
│  ┌──────▼──────┐  ┌──────▼──────┐  ┌──────▼─────┐          │
│  │EngineReg    │  │ConstraintReg│  │  RuleReg   │          │
│  │[DQEngine]   │  │[CustomConst.│  │  [DRule]   │          │
│  │+get_eng_cls │  │             │  │            │          │
│  │(alias)      │  │             │  │            │          │
│  └─────────────┘  └─────────────┘  └────────────┘          │
└──────────────────────────────────────────────────────────────┘
```

#### Impact
- Inconsistent API surface
- Confusing for contributors adding new registries

#### Recommended Solution

1. **Create base registry class**:
```python
class GenericRegistry(Generic[T]):
    _registry: Dict[str, Type[T]] = {}
    _lock = threading.Lock()

    @classmethod
    def register(cls, name: str, item: Type[T]) -> None:
        with cls._lock:
            cls._registry[name.lower()] = item

    @classmethod
    def get(cls, name: str) -> Type[T]:
        with cls._lock:
            if name not in cls._registry:
                available = ', '.join(cls._registry.keys())
                raise KeyError(f"Unknown '{name}'. Available: {available}")
            return cls._registry[name]

    @classmethod
    def unregister(cls, name: str) -> None:
        with cls._lock:
            cls._registry.pop(name.lower(), None)

    @classmethod
    def list_items(cls) -> List[str]:
        return list(cls._registry.keys())
```

2. **Update all registries to inherit**:
```python
class EngineRegistry(GenericRegistry[DQEngine]):
    @classmethod
    def get_engine_class(cls, name: str) -> Type[DQEngine]:
        return cls.get(name)  # Alias for backward compat

    # Add convention discovery logic here

class ConstraintRegistry(GenericRegistry[CustomConstraint]):
    pass  # Inherits all methods

class RuleRegistry(GenericRegistry[DRule]):
    pass  # Inherits all methods
```

#### Files to Modify
- `dq/engine/engine_registry.py`
- `dq/engine/custom/constraint_registry.py`
- `dq/engine/drules/rule_registry.py`
- Create new: `dq/utils/registry_base.py`

#### Estimated Effort
1 day

#### Dependencies
- None

---

### D4: SchemaValidation-Deequ Coupling

**ID**: D4
**Priority**: Medium
**Category**: Design
**Status**: **OPEN**

#### Description
`SchemaValidationEngine` was previously coupled to `DeequEngine`. While the inheritance was removed, the implementation still relies heavily on PyDeequ internals.

#### Current State
```python
# In schemavalidation_check.py
from pydeequ.checks import Check, CheckLevel

def _new_check(self, description="Schema Validation"):
    return Check(
        spark_session=self._spark_session,
        level=CheckLevel.Error,
        description=description,
    )
```

#### Impact
- Schema validation requires PyDeeven though it could be engine-agnostic
- Can't use schema validation without De dependency
- Tight coupling to Deequ's API changes

#### Recommended Solution

1. **Extract validation logic to engine-agnostic layer**:
```python
# dq/validation/schema_validator.py
class SchemaValidator:
    def __init__(self, spark_session, schema_config):
        self._spark = spark_session
        self._config = schema_config

    def validate_datatype(self, df: DataFrame, column: str, expected_type: str) -> ValidationResult:
        """Engine-agnostic datatype validation using Spark native methods"""
        actual_type = str(df.schema[column].dataType)

        if self._types_match(actual_type, expected_type):
            return ValidationResult(success=True, details={...})

        # Use native Spark SQL for validation
        mismatch_count = df.filter(
            f"typeof({column}) != '{self._spark_type_to_sql(expected_type)}'"
        ).count()

        return ValidationResult(
            success=mismatch_count == 0,
            details={"mismatch_count": mismatch_count, "expected": expected_type, "actual": actual_type}
        )
```

2. **Create adapter pattern for engines**:
```python
class DeequSchemaAdapter:
    def __init__(self, validator: SchemaValidator, check_level):
        self._validator = validator
        self._check_level = check_level

    def add_to_check(self, deequ_check, constraint_name, column, **kwargs):
        """Convert validation result to Deequ Check"""
        result = self._validator.validate_datatype(...)
        if not result.success:
            return deequ_check.satisfies(...)
        return deequ_check
```

#### Files to Modify
- Create new: `dq/validation/__init__.py`, `dq/validation/schema_validator.py`
- Create new: `dq/engine/schemavalidation/deequ_adapter.py`
- Modify: `dq/engine/schemavalidation/schemavalidation_check.py`

#### Estimated Effort
3-5 days

#### Dependencies
- D2 (normalized output schema) should be done first

---

### D5: Engine Discovery Fragility

**ID**: D5
**Priority**: Medium
**Category**: Design
**Status**: ✅ **COMPLETED** (2026-02-07)

#### Description
Convention-based engine discovery (`dq.engine.{name}.{name}_engine`) can fail silently or with unclear errors. No validation that the discovered class actually implements `DQEngine`.

#### Issues Fixed
1. ✅ Class name capitalization now handles underscores properly
   - `"schema_validation"` → `"SchemaValidationEngine"` (was `"SchemavalidationEngine"`)
   - `"my_engine"` → `"MyengineEngine"` (was `"My_engineEngine"`)
2. ✅ Added validation that discovered class is actually a `DQEngine` subclass
3. ✅ Added validation for required `apply` method
4. ✅ Enhanced error messages for debugging
5. ✅ Added validation to entry point discovery as well

#### Implementation Details

**Fixed Class Name Logic:**
```python
# Old (broken):
class_name = f"{name.capitalize()}Engine"

# New (correct):
class_name = ''.join(word.capitalize() for word in name.split('_')) + "Engine"
```

**Validation Added:**
```python
# Validate it's actually a class
if not isinstance(engine_class, type):
    raise ImportError(f"Engine '{name}' found '{class_name}' but it is not a class")

# Validate it inherits from DQEngine
from dq.engine.dq_engine import DQEngine
if not issubclass(engine_class, DQEngine):
    raise ImportError(f"Engine '{name}' found '{class_name}' but it does not inherit from DQEngine")

# Validate it has the required 'apply' method
if not hasattr(engine_class, 'apply'):
    raise ImportError(f"Engine '{name}' found '{class_name}' but it is missing the required 'apply' method")
```

#### Files Modified
- `dq/engine/engine_registry.py` - Fixed class name construction and added validation
- `dq/tests/unit/test_engine_loader_unit.py` - Added 11 new tests

#### Test Results
All 146 unit tests pass (135 original + 11 new)

#### Commit
`2f67517` - Implement D5: Engine Discovery Fragility fixes

---

### D6: Config Validation Split

**ID**: D6
**Priority**: Low
**Category**: Design
**Status**: ✅ **COMPLETED** (2026-02-07)

#### Description
Some engines validated config at init time (`DrulesEngine`, `GreatExpectationsEngine`), while others didn't validate until `apply()` was called. This led to late-discovered configuration errors.

#### Current State

| Engine | Validates At | Notes |
|--------|--------------|-------|
| DrulesEngine | `__init__` | ✓ Fail-fast |
| GreatExpectationsEngine | `__init__` | ✓ Fail-fast |
| DeequEngine | `apply()` | ✗ Late validation |
| CustomEngine | `apply()` | ✗ Late validation |
| SchemaValidationEngine | `apply()` via checks | ✗ Late validation |

#### Impact
- Configuration errors discovered only during execution
- Wasted resources loading DataFrames before validation
- Inconsistent user experience

#### Recommended Solution

1. **Add base validation method to `DQEngine`**:
```python
class DQEngine(ABC):
    def __init__(self, config: ConfigTree, dqts: Optional[int] = None):
        self._config = config
        self._dqts = dqts
        self._validate_config()  # Always validate at init

    def _validate_config(self) -> None:
        """Validate configuration at init time. Override in subclasses."""
        pass  # Default: no validation

    @abstractmethod
    def apply(self, dataframe: DataFrame, repository=None) -> List[Dict[str, Any]]:
        """Apply data quality checks to the given DataFrame."""
        raise NotImplementedError("Subclasses must implement this method")
```

2. **Implement validation in each engine**:
```python
# DeequEngine
def _validate_config(self) -> None:
    checks = self._config.get("checks", [])
    if not checks:
        raise ConfigurationError("DeequEngine requires 'checks' in configuration")

    for i, check in enumerate(checks):
        if "constraint" not in check:
            raise ConfigurationError(f"Check at index {i} missing 'constraint' key")

# CustomEngine
def _validate_config(self) -> None:
    checks = self._config.get("checks", [])
    if not checks:
        raise ConfigurationError("CustomEngine requires 'checks' in configuration")

    for i, check in enumerate(checks):
        constraint = check.get("constraint")
        if constraint and not ConstraintRegistry.is_registered(constraint):
            raise ConfigurationError(f"Unknown constraint '{constraint}' at index {i}")
```

#### Files to Modify
- `dq/engine/dq_engine.py` - Add base `_validate_config()`
- `dq/engine/deequ/deequ_engine.py`
- `dq/engine/custom/custom_engine.py`
- `dq/engine/schemavalidation/schemavalidation_engine.py`
- `dq/engine/greatexpectations/greatexpectations_engine.py`

#### Implementation Details

**Added to DQEngine base class:**
```python
class DQEngine(ABC):
    def __init__(self, config: ConfigTree, dqts: Optional[int] = None, ...):
        self._config = config
        self._dqts = dqts
        self._cache = {}
        # ... repository setup ...

        # Validate configuration at init time (fail-fast)
        self._validate_config()

    def _validate_config(self) -> None:
        """Validate configuration at init time.

        Subclasses can override this method to perform engine-specific
        validation. This is called at the end of __init__ to provide
        fail-fast behavior for configuration errors.

        Raises:
            ConfigurationError: If the configuration is invalid.
        """
        pass
```

**Implemented validation in engines:**

1. **DeequEngine**: Validates `checks` exists and each check has `constraint` key
2. **CustomEngine**: Validates `checks` exists, each check has `constraint` key, and constraint is registered
3. **SchemavalidationEngine**: Validates `schema` key exists with table definitions
4. **DrulesEngine**: Already had validation - moved rules import before class definition
5. **GreatExpectationsEngine**: Already had validation - removed redundant explicit call

**Added helper to ConstraintRegistry:**
```python
@classmethod
def is_registered(cls, name: str) -> bool:
    """Check if a constraint is registered."""
    with cls._lock:
        return name in cls._constraints
```

**Files Modified:**
- `dq/engine/dq_engine.py` - Added `_validate_config()` base method, call in `__init__`
- `dq/engine/deequ/deequ_engine.py` - Implemented validation for checks/constraint keys
- `dq/engine/custom/custom_engine.py` - Implemented validation for checks/constraint/registration
- `dq/engine/schemavalidation/schemavalidation_engine.py` - Implemented validation for schema key
- `dq/engine/drules/drules_engine.py` - Moved rules import before class definition
- `dq/engine/greatexpectations/greatexpectations_engine.py` - Removed redundant explicit validation call
- `dq/engine/custom/constraint_registry.py` - Added `is_registered()` method
- `dq/tests/unit/test_config_validation_unit.py` (NEW) - 22 comprehensive tests
- `dq/tests/unit/test_lifecycle_hooks_unit.py` - Fixed config for CustomEngine tests

#### Test Results
All 173 unit tests pass (151 original + 22 new)

#### Commit
`19e8507` - Implement D6: Config validation split for fail-fast behavior

#### Estimated Effort
2 days

#### Dependencies
- None

---

### D7: No Engine Lifecycle Hooks

**ID**: D7
**Priority**: Low
**Category**: Design
**Status**: ✅ **COMPLETED** (2026-02-07)

#### Description
Engines had no way to perform initialization or cleanup before/after rule execution. This made it difficult to:
- Cache expensive operations
- Set up temporary resources (views, tables)
- Clean up resources after execution

#### Current State
```python
# No hook system - everything in apply()
def apply(self, dataframe, repository=None):
    # Can't cache DataFrames across invocations
    # Can't create temp views with guaranteed cleanup
    # No before/after hooks
    return metrics
```

#### Visual Representation

```
┌─────────────────────────────────────────────────────────────────┐
│                    Current State (NO HOOKS)                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  DQFramework.apply(dataframe)                                    │
│       │                                                          │
│       ▼                                                          │
│  engine.apply(dataframe)                                         │
│       │                                                          │
│       ├── [DO CHECKS] ────> metrics                              │
│       │      ❌ Can't cache reference DataFrames                │
│       │      ❌ Can't create temp views                         │
│       │      ❌ Can't cleanup resources                         │
│       │                                                          │
│       └── return metrics                                         │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                   Target State (WITH HOOKS)                       │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  DQFramework.apply(dataframe)                                    │
│       │                                                          │
│       ▼                                                          │
│  engine.apply(dataframe)                                         │
│       │                                                          │
│       ├── before_apply(dataframe)  ◄──────┐                     │
│       │      ✓ Cache reference DataFrames  │                    │
│       │      ✓ Create temp views           │ Setup              │
│       │      ✓ Initialize resources        │                    │
│       │                                   │                     │
│       ├── [DO CHECKS] ────> metrics       │                     │
│       │                                   │                     │
│       ├── after_apply(dataframe, metrics) │                     │
│       │      ✓ Unpersist cached DataFrames │                    │
│       │      ✓ Drop temp views             │ Cleanup            │
│       │      ✓ Release resources          │                     │
│       │                                   │                     │
│       └── return metrics                  │                     │
│                                          │                     │
│  ┌────────────────────────────────────────┴─────────────┐      │
│  │         Resource Lifecycle (Example)                  │      │
│  │                                                     │      │
│  │  before_apply():                                   │      │
│  │    ┌─────────┐                                    │      │
│  │    │ Cache   │ ────> ref_df.cache()               │      │
│  │    └─────────┘                                    │      │
│  │                                                    │      │
│  │  apply_checks():                                  │      │
│  │    ✓ Use cached DataFrames                       │      │
│  │    ✓ Create temp views                           │      │
│  │                                                    │      │
│  │  after_apply():                                   │      │
│  │    ┌─────────┐                                    │      │
│  │    │ Cleanup │ ────> ref_df.unpersist()            │      │
│  │    └─────────┘                                    │      │
│  └────────────────────────────────────────────────────┘      │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

#### Example Use Case: Cached Reference Data

```
Without Hooks (Current):
┌─────────────────────────────────────────────┐
│ Multiple apply() calls on same engine       │
├─────────────────────────────────────────────┤
│ apply() #1: Load ref DF from DB (slow)      │
│ apply() #2: Load ref DF from DB (slow) ❌    │
│ apply() #3: Load ref DF from DB (slow) ❌    │
└─────────────────────────────────────────────┘

With Hooks (Target):
┌─────────────────────────────────────────────┐
│ Multiple apply() calls with caching         │
├─────────────────────────────────────────────┤
│ before_apply(): Load & cache ref DF         │
│ apply() #1: Use cached DF (fast) ✓          │
│ apply() #2: Use cached DF (fast) ✓          │
│ apply() #3: Use cached DF (fast) ✓          │
│ after_apply(): Unpersist cached DF          │
└─────────────────────────────────────────────┘
```

#### Recommended Solution

1. **Add lifecycle hooks to `DQEngine`**:
```python
class DQEngine(ABC):
    def __init__(self, config: ConfigTree, dqts: Optional[int] = None):
        self._config = config
        self._dqts = dqts
        self._cache = {}  # Simple cache for engine-specific data

    def before_apply(self, dataframe: DataFrame) -> None:
        """Called before apply(). Override for setup logic."""
        pass

    def after_apply(self, dataframe: DataFrame, metrics: List) -> None:
        """Called after apply(). Override for cleanup logic."""
        pass

    @abstractmethod
    def apply(self, dataframe: DataFrame, repository=None) -> List[Dict[str, Any]]:
        self.before_apply(dataframe)
        try:
            metrics = self._apply_checks(dataframe, repository)
        finally:
            self.after_apply(dataframe, metrics)
        return metrics

    def _apply_checks(self, dataframe, repository):
        """Actual implementation - override in subclasses"""
        raise NotImplementedError
```

2. **Use hooks in engines**:
```python
# CustomEngine example
def before_apply(self, dataframe: DataFrame):
    # Cache reference DataFrames
    for check in self._config.get("checks", []):
        if check.get("constraint") == "LookupBasedOnColumnNameList":
            ref_table = check.get("ref_table")
            cache_key = f"ref_{ref_table}"
            if cache_key not in self._cache:
                self._cache[cache_key] = self._spark_session.table(ref_table).cache()

def after_apply(self, dataframe: DataFrame, metrics: List):
    # Cleanup cached DataFrames
    for df in self._cache.values():
        df.unpersist()
    self._cache.clear()
```

#### Files to Modify
- `dq/engine/dq_engine.py` - Add lifecycle hooks
- Optional: Implement in individual engines

#### Estimated Effort
1-2 days

#### Implementation Details

**Added to DQEngine base class:**
```python
class DQEngine(ABC):
    def __init__(self, config: ConfigTree, dqts: Optional[int] = None, ...):
        self._cache = {}  # Cache for engine-specific data
        # ...

    def before_apply(self, dataframe: DataFrame) -> None:
        """Hook called before apply() executes."""
        pass

    def after_apply(self, dataframe: DataFrame, metrics: List) -> None:
        """Hook called after apply() completes (even if it fails)."""
        pass
```

**Example implementation in CustomEngine:**
```python
class CustomEngine(DQEngine):
    def before_apply(self, dataframe: DataFrame):
        super().before_apply(dataframe)
        self._spark_session = dataframe.sparkSession
        self._cache_reference_dataframes(dataframe)

    def after_apply(self, dataframe: DataFrame, metrics: List):
        super().after_apply(dataframe, metrics)
        # Unpersist all cached DataFrames
        for cache_key, cached_df in self._cache.items():
            if hasattr(cached_df, "unpersist"):
                cached_df.unpersist()
        self._cache.clear()
```

#### Files Modified
- `dq/engine/dq_engine.py` - Added lifecycle hooks and cache to base class
- `dq/engine/custom/custom_engine.py` - Implemented hooks for DataFrame caching
- `dq/tests/unit/test_lifecycle_hooks_unit.py` (NEW) - 9 comprehensive tests

#### Test Results
All 155 unit tests pass (146 + 9 new)

#### Commit
`c0c46f7` - Implement D7: Engine Lifecycle Hooks

#### Dependencies
- None

---

### D8: CatalogFactory Not Extensible

**ID**: D8
**Priority**: Low
**Category**: Design
**Status**: ✅ **COMPLETED** (2026-02-07)

#### Description
Catalog providers could not be added without modifying `CatalogFactory`. The factory used hard-coded type strings.

#### Implementation Details

**Added registration capability:**
```python
class CatalogFactory:
    _CATALOG_PROVIDERS = {
        "spark": SparkCatalogProvider,
        "hive": SparkCatalogProvider,
        "unity": UnityCatalogProvider,
        "glue": GlueCatalogProvider,
        "delta": UnityCatalogProvider,
    }
    _lock = threading.Lock()

    @classmethod
    def register_provider(cls, catalog_type: str, provider_class):
        """Register a custom catalog provider."""
        if not issubclass(provider_class, CatalogProvider):
            raise TypeError(f"{provider_class.__name__} must inherit from CatalogProvider")

        catalog_type = catalog_type.lower().strip()
        with cls._lock:
            cls._CATALOG_PROVIDERS[catalog_type] = provider_class

    @classmethod
    def supported_types(cls):
        """Return list of supported catalog type names."""
        return list(cls._CATALOG_PROVIDERS.keys())
```

**Usage example:**
```python
class MyCatalogProvider(CatalogProvider):
    def __init__(self, spark_session):
        super().__init__(spark_session)
        # Custom implementation

# Register at application startup
CatalogFactory.register_provider("my_system", MyCatalogProvider)

# Now usable in config: dqframework { catalog_type = "my_system" }
```

#### Files Modified
- `dq/catalog/catalog_factory.py` - Added register_provider() and thread safety
- `dq/tests/unit/test_catalog_factory_unit.py` - 8 comprehensive tests

#### Test Results
All 163 unit tests pass (155 + 8 new)

#### Commit
`ede0660` - Implement D8: CatalogFactory extensibility with custom provider registration

---

## Vision Items (Longer-Term Enhancements)

### V1: Multi-Engine Support Beyond Spark

**ID**: V1
**Priority**: Low (Vision)
**Category**: Vision
**Status**: **OPEN**

#### Description
Framework currently only supports Spark DataFrames. Could extend to:
- Pandas/Dask DataFrames
- Polars DataFrames
- Database-native validation (push down SQL to warehouse)
- REST API-based validation

#### Recommended Approach
1. Abstract DataFrame interface behind duck typing or protocol
2. Create engine variants per execution engine
3. Share validation logic where possible

#### Estimated Effort
3-6 months

---

### V2: Observability & Metrics Export Integration

**ID**: V2
**Priority**: Medium (Vision)
**Category**: Vision
**Status**: **OPEN**

#### Description
No native integration with observability platforms. Metrics are stored locally but not exported to:
- Prometheus
- Datadog
- CloudWatch
- Grafana Loki
- OpenTelemetry

#### Recommended Approach
1. Define metrics export interface
2. Create adapters for common platforms
3. Add configuration for export destinations

#### Estimated Effort
2-4 weeks

---

### V3: Streaming Data Support

**ID**: V3
**Priority**: Low (Vision)
**Category:** Vision
**Status**: **OPEN**

#### Description
Framework processes batch DataFrames only. No support for:
- Spark Structured Streaming
- Kafka/Kinesis data streams
- Continuous validation
- Windowed aggregations

#### Recommended Approach
1. Create `StreamingDQFramework` class
2. Implement foreachBatch processor pattern
3. Add watermarking and state management

#### Estimated Effort
4-6 weeks

---

### V4: Remediation Capabilities

**ID**: V4
**Priority**: Low (Vision)
**Category**: Vision
**Status**: **OPEN**

#### Description
Framework detects issues but can't fix them. Could add:
- Automatic data cleansing
- Value imputation
- Outlier handling strategies
- Retry mechanisms for transient failures

#### Recommended Approach
1. Define remediation strategies per check type
2. Add action configuration (dry-run vs remediate)
3. Implement idempotent remediation operations

#### Estimated Effort
6-8 weeks

---

### V5: Distributed Orchestration

**ID**: V5
**Priority**: Low (Vision)
**Category**: Vision
**Status**: **OPEN**

#### Description
Framework runs on single Spark driver. Could add:
- Airflow DAG generator
- Databricks Workflow integration
- Kubeflow Pipelines support
- Distributed task scheduling

#### Recommended Approach
1. Create workflow DSL generator from DQ config
2. Build integrations with common orchestrators
3. Add execution metadata tracking

#### Estimated Effort
4-6 weeks

---

### V6: Data Profiling Features

**ID**: V6
**Priority**: Medium (Vision)
**Category**: Vision
**Status**: **OPEN**

#### Description
No native profiling capabilities. Users must manually:
- Analyze data distributions
- Identify patterns
- Discover quality issues proactively

#### Recommended Approach
1. Add `ProfilerEngine` with built-in profiling checks
2. Generate statistical summaries
3. Suggest rules based on profile data
4. Export profile reports

#### Estimated Effort
3-4 weeks

---

## Priority Matrix

### Quick Wins (COMPLETED)
- ~~**D5**: Engine Discovery Fragility~~ ✅
- **D7**: Engine Lifecycle Hooks ✅
- **D8**: CatalogFactory Extensibility ✅

### Medium Effort (2-5 days each)
- **D2**: Return Type Variance
- **D6**: Config Validation Split
- **V6**: Data Profiling Features

### Larger Efforts (3+ weeks each)
- **D4**: SchemaValidation-Deequ Decoupling
- **V2**: Observability Integration
- **V3**: Streaming Support
- **V4**: Remediation Capabilities
- **V5**: Distributed Orchestration
- **V1**: Multi-Engine Support

---

## How to Use This Tracker

### For Contributors
1. Pick an item from the tracker
2. Create a branch: `feature/d<id>-<short-name>`
3. Implement according to "Recommended Solution"
4. Update status to `IN_PROGRESS`
5. Submit PR with reference to tracker ID

### For Maintainers
1. Review items in priority order
2. Assign to contributors based on effort
3. Update status as work progresses
4. Mark items as `COMPLETED` when merged

### Status Values
- **OPEN**: Not started, available for assignment
- **IN_PROGRESS**: Someone is working on it
- **BLOCKED**: Waiting for dependency
- **COMPLETED**: Done and merged
- **DEFERRED**: Postponed indefinitely

---

## Changelog

| Date | Change | Author |
|------|--------|--------|
| 2026-02-07 | Completed D2: Return Type Variance | Claude (Sonnet 4.5) |
| 2026-02-07 | Completed D6: Config Validation Split | Claude (Sonnet 4.5) |
| 2026-02-07 | Completed D8: CatalogFactory Extensibility | Claude (Sonnet 4.5) |
| 2026-02-07 | Completed D7: Engine Lifecycle Hooks | Claude (Sonnet 4.5) |
| 2026-02-07 | Completed D5: Engine Discovery Fragility | Claude (Sonnet 4.5) |
| 2026-02-07 | Initial tracker creation, completed Phases A-D | Claude (Sonnet 4.5) |
| | | |
