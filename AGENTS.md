# Agent Guidelines for Strata

Coding conventions and patterns for AI agents working on this codebase.

## Import Style

**All imports apart from Python's standard library should use the `import ... as ...` pattern:**

```python
# CORRECT
import strata.core as core
import strata.sources as sources
import strata.errors as errors
import strata.plugins.base as base
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
├── core.py          # SDK primitives: Entity, Field, Schema, FeatureTable, etc.
├── sources.py       # BatchSource, StreamSource, RealTimeSource
├── settings.py      # Configuration loading (strata.yaml)
├── errors.py        # StrataError and subclasses
├── checks.py        # SLA definitions
├── types.py         # PyArrow type aliases
├── cli.py           # CLI commands
└── plugins/
    ├── base.py      # BaseRegistry, BaseStorage, BaseCompute, BaseSourceConfig
    ├── duckdb/      # DuckDB implementations
    ├── local/       # Local file implementations
    └── sqlite/      # SQLite implementations
```

## Plugin Architecture

Plugins are organized by category:
- **Storage**: Data reading/writing (BaseStorage, BaseSourceConfig)
- **Compute**: Query execution (BaseCompute)
- **Registry**: Metadata storage (BaseRegistry)

Source configs belong in plugins, not a separate integrations file:
```python
from strata.plugins.duckdb import DuckDBSourceConfig
from strata.plugins.local.storage import LocalSourceConfig
```

## Public API

The public API is minimal. Users only need:
- `strata.Entity`, `strata.Field`, `strata.Schema`
- `strata.FeatureTable`, `strata.SourceTable`, `strata.Feature`
- `strata.Dataset`
- `strata.BatchSource`, `strata.StreamSource`, `strata.RealTimeSource`

Do NOT export: errors, settings, internal utilities.

## Error Messages

Follow the context + cause + fix pattern (DX-01):

```python
raise errors.StrataError(
    context="Validating entity 'user'",
    cause="join_keys cannot be empty",
    fix="Provide at least one join key for the entity.",
)
```

## Pydantic Models

- Use `StrataBaseModel` from `strata.core` as base class
- Plugin configs use strict, frozen models: `pdt.BaseModel, strict=True, frozen=True, extra="forbid"`
- Use `pdt.Field(alias="...")` for reserved words (e.g., `schema_` with alias `"schema"`)

## Development Commands

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
