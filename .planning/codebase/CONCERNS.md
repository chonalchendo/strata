# Codebase Concerns

**Analysis Date:** 2026-02-02

## Tech Debt

**Incomplete Abstract Implementations:**
- Issue: `BaseRegistry` defines 6 abstract methods but `SqliteRegistry` is a stub
- Files: `src/strata/plugins/base.py:42-66`, `src/strata/plugins/sqlite/registry.py:6-8`
- Impact: Cannot use registry for state management
- Fix: Implement all abstract methods in SqliteRegistry

**Decorator Return Type Uncertainty:**
- Issue: FeatureTable methods have unclear return types with TODO comments
- Files: `src/strata/core.py:23-45`
- Impact: Feature transformation API is non-functional
- Fix: Define clear contracts, implement transform/feature/aggregate methods

**Empty Stubs Throughout:**
- Issue: Multiple empty classes and methods
- Files: `src/strata/core.py`, `src/strata/sources.py`, `src/strata/plugins/base.py`
- Impact: No validation or common behavior in hierarchy
- Fix: Add base validation logic and common methods

**Incomplete CLI Commands:**
- Issue: `new()`, `down()`, `build()` are empty stubs
- Files: `src/strata/cli.py:14-66`
- Impact: Core CLI functionality non-functional
- Fix: Implement full workflow for each command

**Unimplemented Dataset Methods:**
- Issue: `get_offline_features()` and `get_online_vector()` are empty
- Files: `src/strata/core.py:80-88`
- Impact: Cannot retrieve features for training or serving
- Fix: Implement query logic with proper filtering

## Known Bugs

**Python Version Requirement:**
- Issue: Requires Python 3.14.2 (unreleased)
- Files: `pyproject.toml:9`, `.python-version`
- Workaround: Change to Python >=3.10

## Test Coverage Gaps

**No Tests Exist:**
- What's not tested: Entire codebase
- Risk: Cannot verify functionality, breaking changes are silent
- Priority: Critical

## Security Considerations

**No Input Validation in Settings:**
- Risk: `load_strata_settings()` loads YAML without size limits
- Files: `src/strata/settings.py:46-53`
- Recommendation: Add file size limits, validate YAML structure

**No Access Control on Registry:**
- Risk: No authentication/authorization on state operations
- Files: `src/strata/plugins/base.py:42-66`
- Recommendation: Add role-based access control

## Fragile Areas

**State Management:**
- Files: `src/strata/plugins/base.py`, `src/strata/plugins/sqlite/registry.py`
- Why fragile: No implementation, unclear transaction semantics
- Safe modification: Create tests first, define clear invariants

**Feature Definition API:**
- Files: `src/strata/core.py`
- Why fragile: Unclear decorator semantics, unimplemented methods
- Safe modification: Write integration tests first

**Plugin System:**
- Files: `src/strata/plugins/factory.py`, `src/strata/plugins/base.py`
- Why fragile: Discriminated unions require exact matching
- Safe modification: Add type validation tests

## Missing Critical Features

**No Schema Enforcement:**
- `Schema` class is empty stub
- Cannot validate data quality or detect drift

**No Feature Lineage:**
- Cannot track source-to-feature dependencies
- Blocks impact analysis

**No Data Quality Checks:**
- `SLA` class defined but no execution engine
- Cannot monitor feature health

**No Feature Serving:**
- `get_offline_features()` and `get_online_vector()` unimplemented
- Core use case blocked

## Scaling Limits

**No Pagination:**
- Current: Single batch load into memory
- Limit: Fails on large tables

**Local Storage Only:**
- Current: Files on local filesystem
- Limit: Cannot scale to multi-machine

---

*Concerns audit: 2026-02-02*
