# Technology Stack

**Analysis Date:** 2026-02-02

## Languages

**Primary:**
- Python 3.14.2 - Core language for entire codebase
- YAML - Configuration files for strata.yaml

## Runtime

**Environment:**
- Python 3.14.2 (specified in `.python-version`)

**Package Manager:**
- uv (modern Python package manager)
- Lockfile: `uv.lock` present
- Build system: `uv_build>=0.8.8,<0.9.0`

## Frameworks

**Core Framework:**
- Pydantic 2.12.5+ - Data validation and settings management
- Pydantic Settings 2.12.0+ - Configuration management via `BaseSettings`

**CLI:**
- Cyclopts 4.5.1+ - Command-line interface framework (`src/strata/cli.py`)

**Data Processing:**
- PyArrow 23.0.0+ - Arrow table format for data interchange
- DuckDB 1.4.4+ (optional) - SQL compute engine
- Ibis Framework 11.0.0+ (optional, DuckDB backend) - Data abstraction layer

**Data Formats:**
- Delta Lake 1.4.1+ - Lakehouse table format

**Configuration:**
- OmegaConf 2.3.0+ - Configuration file parsing (`src/strata/settings.py`)

**Utilities:**
- Loguru 0.7.3+ - Structured logging
- Rich 14.3.1+ - Rich text and formatting for CLI output

## Testing & Quality

**Testing:**
- pytest 9.0.2+ - Test runner and framework

**Linting & Code Quality:**
- ruff 0.14.14+ - Fast Python linter and formatter
- ty 0.0.14 - Type checking tool

## Key Dependencies

**Critical Infrastructure:**
- `pydantic>=2.12.5` - Core validation and type system
- `pyarrow>=23.0.0` - Arrow table format for internal data representation
- `cyclopts>=4.5.1` - CLI command parsing and execution
- `omegaconf>=2.3.0` - YAML configuration parsing

**Data Management:**
- `deltalake>=1.4.1` - Delta Lake storage integration
- `ibis-framework[duckdb]>=11.0.0` - SQL query abstraction (optional)
- `duckdb>=1.4.4` - SQL query execution engine (optional)

## Configuration

**Environment:**
- Configuration via YAML files (`strata.yaml`)
- Three environment configurations supported: `dev`, `stg`, `prd`

**Build:**
- `pyproject.toml` - Standard Python project metadata
- Build backend: `uv_build`
- Workspace configuration with member: `examples/air-quality`

**Configuration Files:**
- `pyproject.toml` - Main project configuration
- `uv.lock` - Dependency lock file
- `.python-version` - Python version specification
- `strata.yaml` - Runtime configuration

## Entry Points

**CLI:**
- `strata.cli:app` - Main CLI application
- Commands: `new`, `preview`, `up`, `down`, `build`

**Python Package:**
- `strata` - Main package exports:
  - Core: `FeatureTable`, `Feature`, `Dataset`, `Entity`, `Schema`, `Field`, `SourceTable`
  - Sources: `BatchSource`, `StreamSource`, `RealTimeSource`

---

*Stack analysis: 2026-02-02*
