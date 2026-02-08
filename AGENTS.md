# Agent Guidelines for Strata

Coding conventions and patterns for AI agents working on this codebase.

## Project Overview

Strata is a DuckDB-first Python feature store providing training-serving consistency. Users define features declaratively in Python, Strata handles compilation (via Ibis), materialization (via DuckDB + Delta/Parquet), and data quality enforcement.

**Current state:** Phases 1-5 complete. Phase 6 (Dataset & Serving) is next.

## Import Style

**All imports apart from Python's standard library should use the `import ... as ...` pattern:**

```python
# CORRECT
import strata.core as core
import strata.sources as sources
import strata.errors as errors
import strata.backends.base as base
import pydantic as pdt
import pyarrow as pa

entity = core.Entity(name="user", join_keys=["user_id"])
```

```python
# WRONG - do not use direct imports
from strata.core import Entity, Field, FeatureTable
from strata.errors import StrataError
from pydantic import BaseModel
```

This keeps imports explicit and avoids namespace pollution.

## Project Structure

```
src/strata/
├── __init__.py          # Public API exports
├── __main__.py          # CLI entry point
├── core.py              # SDK primitives: Entity, Field, Schema, FeatureTable, SourceTable, Dataset, Feature
├── sources.py           # BatchSource, StreamSource, RealTimeSource
├── settings.py          # Configuration loading (strata.yaml via OmegaConf)
├── errors.py            # StrataError with context/cause/fix + to_dict() for JSON
├── checks.py            # SLA model (max_staleness, min_row_count, severity)
├── types.py             # PyArrow type aliases
├── cli.py               # CLI commands (cyclopts + Rich)
├── compiler.py          # IbisCompiler: feature definitions → SQL via Ibis
├── compile_output.py    # Write query.sql, ibis_expr.txt, lineage.json, build_context.json
├── build.py             # BuildEngine: DAG-ordered materialization with quality integration
├── dag.py               # DAG resolution: Kahn's algorithm, topological sort
├── formats.py           # BaseFormat ABC, DeltaFormat, ParquetFormat (shared across backends)
├── schema_evolution.py  # detect_schema_changes: type widening/narrowing detection
├── discovery.py         # DefinitionDiscoverer: find Python feature definitions
├── diff.py              # Diff engine: compare definitions vs registry state
├── registry.py          # Registry types and interfaces
├── validation.py        # Definition validation (duplicates, invalid refs, missing entities)
├── output.py            # Rich output helpers (Pulumi-style diff rendering)
├── quality.py           # BaseConstraintChecker ABC, PyArrowConstraintChecker, validate_table()
├── freshness.py         # check_freshness(), TableFreshness, FreshnessResult
└── backends/
    ├── __init__.py      # Backend exports
    ├── base.py          # BaseBackend ABC (single abstraction per deployment target)
    ├── factory.py       # BackendKind discriminated union
    ├── duckdb/
    │   ├── backend.py   # DuckDBBackend: delegates I/O to format
    │   ├── source.py    # DuckDB source config
    │   ├── storage.py   # DuckDB storage implementation
    │   └── registry.py  # DuckDB registry (unused, SQLite is primary)
    ├── sqlite/
    │   ├── registry.py  # SQLiteRegistry: objects, changelog, meta, quality_results, builds tables
    │   └── source.py    # SQLite source config
    └── local/
        └── storage.py   # Local filesystem storage, LocalSourceConfig
```

**Key architectural decisions:**
- `backends/` (not `plugins/`) — renamed in Phase 4
- `BaseBackend` replaces the earlier `BaseStorage` + `BaseCompute` split
- `BackendKind` replaces `StorageKind` + `ComputeKind` in factory.py
- Formats live at root level (`formats.py`), shared across all backends
- DuckDBBackend delegates I/O to `format.read()`/`format.write()`

## Backend Architecture

Single `BaseBackend` abstraction per deployment target (not separate storage + compute):

```python
import strata.backends.base as base

class BaseBackend(abc.ABC):
    def connect(self) -> ibis.BaseBackend: ...
    def register_source(self, name, path) -> None: ...
    def write_table(self, name, data, mode) -> None: ...
    def read_table(self, name) -> pa.Table: ...
    def drop_table(self, name) -> None: ...
    def table_exists(self, name) -> bool: ...
```

Format abstraction (root-level, backend-agnostic):

```python
import strata.formats as formats

class BaseFormat(abc.ABC):
    def read(self, path) -> pa.Table: ...
    def write(self, path, data, mode) -> None: ...

# Implementations: ParquetFormat, DeltaFormat
```

## Compiler & Build Pipeline

**Data flow:**

```
Feature Definition (Python)
  → IbisCompiler (compiler.py) → Ibis Expression → SQL (dialect-specific)
  → BuildEngine (build.py) → DAG resolution (dag.py) → Execute SQL via DuckDB
  → Quality validation (quality.py) → validate-before-write
  → Format write (formats.py) → Delta/Parquet storage
```

**Key modules:**
- `compiler.py`: `IbisCompiler` — compiles FeatureTable definitions (aggregates, @feature, @transform) to Ibis expressions, then to SQL via `ibis.to_sql(dialect='duckdb')`
- `build.py`: `BuildEngine` — frozen Pydantic model, DAG-ordered materialization, quality integration, build record persistence
- `dag.py`: Kahn's algorithm with sorted queues for deterministic topological sort
- `compile_output.py`: Writes compilation artifacts to `.strata/` (query.sql, ibis_expr.txt, lineage.json, build_context.json)
- `schema_evolution.py`: Detects type widening/narrowing across int/uint/float families

## Data Quality (Phase 5)

**Validation engine:**
- `quality.py`: `BaseConstraintChecker` ABC with `PyArrowConstraintChecker` implementation
- Constraints: `ge`, `le`, `not_null`, `max_null_pct`, `allowed_values`, `pattern`
- Field severity defaults "error" (hard fail); SLA severity defaults "warn" (informational)
- Custom validators via Python callables (always error severity)
- Null-safe numeric checks: ge/le drop nulls before comparison

**Freshness monitoring:**
- `freshness.py`: `check_freshness()` compares worst-of build/data staleness against SLA thresholds
- Tables without SLAs always "fresh" (no threshold to compare against)
- Exit code 1 only for error-severity staleness

**Build integration:**
- Quality validation runs before writes (validate-before-write pattern)
- Fail-fast: error-severity failures abort the build and skip downstream tables
- Best-effort persistence: quality/build record failures logged as warnings

## CLI Commands

Built with **cyclopts**, **Rich** (output), and **loguru** (logging).

| Command | Description |
|---------|-------------|
| `strata new` | Create a new Strata project (not yet implemented) |
| `strata env [name]` | Show current environment or environment details |
| `strata env-list` | List available environments |
| `strata preview` | Preview registry changes (diff without applying) |
| `strata validate` | Check definitions for errors |
| `strata up` | Sync definitions to registry (with `--dry-run`, `--yes`) |
| `strata build [table]` | Materialize tables in DAG order (with `--start`, `--end`, `--schedule`, `--full-refresh`, `--skip-quality`) |
| `strata compile [table]` | Generate SQL to `.strata/` directory |
| `strata down [kind] [name]` | Remove objects from registry |
| `strata ls [kind]` | List registered objects |
| `strata quality <table>` | Run data quality checks (with `--live`, `--json`) |
| `strata freshness` | Check data staleness against SLAs |

**Global flags:**
- `--json` — suppresses Rich output for clean machine-readable stdout
- `-v` / `--verbose` — enables DEBUG logging + loguru
- `--env` — target environment (on most commands)

**CLI patterns:**
- Deferred imports for heavy modules (ibis, pyarrow, build, quality, freshness) to keep `strata --help` fast
- Rich `console.status()` spinners for discovery/validation, suppressed in `--json` mode
- Structured JSON errors: `{error, code, context, cause, fix}` via `StrataError.to_dict()`

## Public API

The public API is minimal. Users only need:

```python
# Core primitives
from strata import Entity, Field, Schema, FeatureTable, SourceTable, Feature, Dataset

# Sources
from strata import BatchSource, StreamSource, RealTimeSource

# Source configs
from strata import LocalSourceConfig
```

Do NOT export: errors, settings, compiler, build engine, internal utilities.

## Error Messages

Follow the context + cause + fix pattern:

```python
raise errors.StrataError(
    context="Validating entity 'user'",
    cause="join_keys cannot be empty",
    fix="Provide at least one join key for the entity.",
)
```

Errors support JSON serialization via `error.to_dict()` for `--json` mode.

## Pydantic Models

- Use `StrataBaseModel` from `strata.core` as base class for domain models
- Backend/plugin configs use strict, frozen models: `pdt.BaseModel, strict=True, frozen=True, extra="forbid"`
- Use `pdt.Field(alias="...")` for reserved words (e.g., `schema_` with alias `"schema"`)
- `BuildEngine` is a frozen Pydantic model (uses `model_construct` in tests to bypass discriminator validation with mocks)
- Discriminated unions via `discriminator="kind"` field (e.g., `BackendKind`, `RegistryKind`)

## Test Conventions

- ~370 tests across 34 test files
- Test files mirror source modules: `test_compiler.py`, `test_build.py`, `test_quality.py`, etc.
- CLI tests use cyclopts test runner
- `test_build.py` uses `autouse _patch_compiler` fixture to work around a pre-existing `decimal.InvalidOperation` in sqlglot on Python 3.14
- Backend tests use `model_construct()` to bypass `BackendKind` discriminator with mock backends

## Development Commands

**IMPORTANT: Environment setup before running any commands.**

Tools like `just`, `ruff`, `pytest`, etc. are installed in the project's virtual environment. Before running any command, you must ensure dependencies are available. The simplest approach is to prefix commands with `uv run`, which automatically syncs dependencies and runs within the virtual environment:

```bash
uv run just check-code    # Preferred — auto-syncs and runs
uv run just check-test
uv run just format
```

Alternatively, activate the virtual environment first:

```bash
uv sync                    # Sync dependencies (creates .venv if needed)
source .venv/bin/activate  # Activate the virtual environment
just check-code            # Now commands work directly
```

If you get errors like `just: command not found` or `ruff: command not found`, it means the virtual environment is not active. Always use `uv run` as the default.

Run commands via `just <command>`. Use `just --list` to see all available commands.

### Code Quality Checks

| Command | Description |
|---------|-------------|
| `just check` | Run all checks (code, type, format, security, coverage) |
| `just check-code` | Lint with ruff |
| `just check-type` | Type check with ty |
| `just check-format` | Verify formatting |
| `just check-security` | Security scan with bandit |
| `just check-test` | Run pytest |
| `just check-coverage` | Run tests with coverage (80% threshold) |

### Formatting

| Command | Description |
|---------|-------------|
| `just format` | Run all formatters |
| `just format-import` | Sort imports with ruff |
| `just format-source` | Format code with ruff |

### Packaging

| Command | Description |
|---------|-------------|
| `just package` | Build wheel with constraints |
| `just package-constraints` | Generate constraints.txt with hashes |
| `just package-build` | Build wheel |

### Cleanup

| Command | Description |
|---------|-------------|
| `just clean-build` | Remove dist/ and build/ |
| `just clean-constraints` | Remove constraints.txt |

### Git Hooks

| Command | Description |
|---------|-------------|
| `just install-hooks` | Install pre-push and commit-msg hooks |
| `just commit-prek` | Run pre-commit checks on staged files |

### Workflow

Before committing:
```bash
just format      # Fix formatting
just check       # Verify all checks pass
```

For tests only:
```bash
just check-test                      # Run tests
just check-coverage cov_fail_under=90  # Custom coverage threshold
```

## Technology Stack

| Package | Purpose |
|---------|---------|
| `pydantic>=2.12.5` | Validation, config, and domain models |
| `pyarrow>=23.0.0` | Data interchange format |
| `ibis-framework[duckdb]>=11.0.0` | Query compilation (Python → SQL) |
| `duckdb>=1.4.4` | SQL compute engine |
| `deltalake>=1.4.1` | Delta Lake storage (delta-rs) |
| `cyclopts>=4.5.1` | CLI framework |
| `rich>=14.3.1` | Tables, trees, spinners, formatting |
| `loguru>=0.7.3` | Structured logging |
| `omegaconf>=2.3.0` | YAML configuration parsing |
| `ruff>=0.14.14` | Linting and formatting |
| `ty>=0.0.14` | Type checking |
| `pytest>=9.0.2` | Test runner |

## What's Next (Phase 6: Dataset & Serving)

- `Dataset.load()`: Point-in-time joins (ASOF) for training data
- `Dataset.lookup()`: Low-latency feature serving via online store
- `BaseOnlineStore`: Separate abstraction from `BaseBackend` (see ADR-001 in `.planning/ARCHITECTURE-DECISIONS.md`)
- SQLite online store for v0.1 (Redis/Postgres deferred)
- `strata publish` CLI command for online store sync
