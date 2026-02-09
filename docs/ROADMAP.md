# Data Quality Framework - Roadmap

## Completed Phases (Previous Work)

### Phase 1: SQL Injection Fix — Unity Catalog ✓
- Added `_escape_identifier()` method to sanitize SQL identifiers
- Applied to all SQL execution points (DESCRIBE, SHOW, USE)
- Added unit tests for identifier escaping

### Phase 2: SchemaValidation Deduplication ✓
- Refactored `_apply_spark_datatype_checks()` to eliminate code duplication
- Extracted helper methods: `_apply_pydeequ_datatype()`, `_apply_cast_datatype()`, `_apply_parameter_constraints()`, `_add_constraint()`
- Removed commented-out code

### Phase 3: Lazy Import Decoupling ✓
- Moved `pyspark`/`pydeequ` imports behind `TYPE_CHECKING` for all engine and constraint files
- Modules can now be imported without Spark runtime available

### Phase 4: repository_utils.py Cleanup ✓
- Removed `df.show()` side effect
- Removed duplicate "json" format
- Renamed `doesTableExistAlready` → `table_exists_already`
- Used `os.path.join()` for path construction

### Phase 5: Logger f-strings ✓
- Fixed logger calls to use `%s` formatting instead of f-strings

### Phase 6: Dead Code Removal ✓
- Removed unused `dq/utils/resilience.py`

### Phase 7: Custom Engine Renaming ✓
- Renamed `dq/engine/custom/` → `dq/engine/constraint/`
- Renamed `custom_engine.py` → `constraint_engine.py`
- Updated all references from "custom" to "constraint"

---

## Current Priorities

### Phase 8: Security Fixes — SQL Injection & eval() Issues ✓

**Status: COMPLETED**

#### 8.1: Fix SQL Injection in lookup_column_list.py ✓
- Added `_escape_identifier()` helper function
- Applied backtick escaping to `ref_columns` and `ref_table`
- Added `# nosec B608` comment with documentation

#### 8.2: Fix SQL Injection in schemavalidation_check.py ✓
- Added `_escape_identifier()` helper function
- Applied backtick escaping to `ref_column`, `ref_column_alias`, and `table_name`
- Added `# nosec B608` comment with documentation

#### 8.3: Document eval() Usage ✓
- Added detailed comments explaining why `eval()` is safe in:
  - `dq/engine/deequ/deequ_check.py:91` - AST-validated lambda evaluator
  - `dq/engine/deequ/deequ_constraints_builder.py:158` - LLM constraint suggestions
- Added `# nosec B307` comments with security documentation
- Updated pyproject.toml comments to explain B307 and B608 skips

#### 8.4: Bandit Configuration ✓
- Confirmed B307 and B608 remain in skips list with proper documentation
- All `eval()` and SQL usage now properly documented and secured

---

## Future Enhancements (Lower Priority)

### Phase 9: Code Quality Improvements ✓ (In Progress)

#### 9.1: Type Hint Coverage ✓
- Fixed List to Sequence invariance for metrics exporters
- Added explicit type annotations to variables in:
  * Constraint files (distinctness_by_group, rate_of_change)
  * Metrics registry
  * DQEngine cache
- Fixed profiler type issues:
  * Specific variable names for different stats types
  * Added null checks for Row indexing
- Reduced mypy errors from 40+ to 33

#### 9.2: Documentation
- Add API documentation with Sphinx
- Add more examples in docs/

#### 9.3: Performance Optimization
- Profile and optimize hot paths
- Consider caching for repeated operations

---

## Testing Coverage

### Current Status
- Unit tests: 253 passing
- Integration tests: 11 passing
- Total: 264 tests passing

### Coverage Goals
- Maintain >90% code coverage
- Add tests for edge cases and error paths

---

## Dependency Management

### Current Status
- Core dependencies minimal and lightweight
- Optional dependencies properly separated
- Python 3.11+ support

### Future
- Consider adding Python 3.13 support
- Regular dependency updates for security patches

---

## Release Planning

### v2.1.0 (Upcoming)
- Security fixes (Phase 8)
- Bug fixes and improvements

### v2.2.0 (Future)
- New features and enhancements
- Performance improvements

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines on contributing to the framework.
