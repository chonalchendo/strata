# Codebase Structure

**Analysis Date:** 2026-02-02

## Directory Layout

```
strata/
├── src/strata/                      # Main framework package
│   ├── __init__.py                  # Public exports
│   ├── __main__.py                  # CLI entry point
│   ├── core.py                      # Core domain abstractions
│   ├── sources.py                   # Source type definitions
│   ├── checks.py                    # SLA and validation constraints
│   ├── cli.py                       # CLI command definitions
│   ├── settings.py                  # Configuration loading
│   └── plugins/                     # Pluggable infrastructure
│       ├── base.py                  # Abstract base classes
│       ├── factory.py               # Plugin type unions
│       ├── duckdb/                  # DuckDB compute engine
│       │   └── compute.py
│       ├── sqlite/                  # SQLite registry backend
│       │   └── registry.py
│       └── local/                   # Local filesystem storage
│           └── storage.py
├── examples/air-quality/            # Example project
│   ├── src/air_quality/tables/      # Feature definitions
│   ├── data/                        # Local data files
│   ├── scripts/                     # Utility scripts
│   └── strata.yaml                  # Environment configuration
├── pyproject.toml                   # Project metadata
└── README.md                        # Documentation
```

## Key File Locations

**Entry Points:**
- `src/strata/__main__.py`: Python module entry
- `src/strata/cli.py`: Cyclopts CLI application

**Configuration:**
- `strata.yaml`: Project root - environment settings
- `src/strata/settings.py`: Configuration loading

**Core Logic:**
- `src/strata/core.py`: Domain abstractions
- `src/strata/sources.py`: Source type definitions
- `src/strata/plugins/base.py`: Plugin base classes

**Infrastructure:**
- `src/strata/plugins/sqlite/registry.py`: Registry backend
- `src/strata/plugins/local/storage.py`: Storage backend
- `src/strata/plugins/duckdb/compute.py`: Compute engine

## Naming Conventions

**Files:** snake_case (`core.py`, `cli.py`, `settings.py`)

**Classes:** PascalCase (`FeatureTable`, `BaseRegistry`, `DuckDBCompute`)

**Functions:** snake_case (`load_strata_settings`, `refresh_state`)

**Directories:** lowercase (`plugins/duckdb`, `plugins/sqlite`)

## Where to Add New Code

**New Plugin:**
- Location: `src/strata/plugins/{plugin_name}/`
- Inherit from `Base{Registry|Storage|Compute}`
- Register in `src/strata/plugins/factory.py`

**New Domain Model:**
- Location: `src/strata/core.py`
- Inherit from `StrataBaseModel`
- Export in `src/strata/__init__.py`

**New CLI Command:**
- Location: `src/strata/cli.py`
- Use `@app.command` decorator

**New Example Project:**
- Location: `examples/{project_name}/`
- Add to `pyproject.toml` workspace members

---

*Structure analysis: 2026-02-02*
