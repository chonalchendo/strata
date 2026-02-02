# Architecture

**Analysis Date:** 2026-02-02

## Pattern Overview

**Overall:** Pluggable feature engineering framework with layered architecture

**Key Characteristics:**
- Domain-driven design with core abstractions (FeatureTable, Entity, Feature, Dataset)
- Plugin-based infrastructure layer supporting Registry, Storage, and Compute
- Configuration-driven environment management (dev, stg, prd)
- Pydantic-based validation throughout
- CLI-driven deployment workflow

## Layers

**Domain/Core Layer:**
- Purpose: Define feature engineering concepts
- Location: `src/strata/core.py`, `src/strata/sources.py`
- Contains: FeatureTable, Feature, Entity, Schema, Field, SourceTable, Dataset

**Source Configuration Layer:**
- Purpose: Abstract data source configuration
- Location: `src/strata/sources.py`
- Contains: BaseSource, BatchSource, StreamSource, RealTimeSource

**Infrastructure Plugin Layer:**
- Purpose: Pluggable registry, storage, and compute
- Location: `src/strata/plugins/`
- Contains: Base abstractions and implementations

**Settings/Configuration Layer:**
- Purpose: Environment-specific configuration from YAML
- Location: `src/strata/settings.py`
- Contains: StrataSettings, EnvironmentSettings, load_strata_settings()

**CLI/Orchestration Layer:**
- Purpose: Command interface for deployment
- Location: `src/strata/cli.py`
- Contains: Cyclopts commands (new, preview, up, down, build)

## Data Flow

**Project Definition Flow:**
1. User creates Python modules in `src/[project]/tables/`
2. User defines Entity, Source, Schema, SourceTable using strata abstractions
3. Strata validates definitions through Pydantic models

**Infrastructure Deployment Flow:**
1. User runs `strata preview --env dev` or `strata up`
2. CLI loads `strata.yaml` via settings
3. Settings validates config and instantiates plugins
4. Registry plugin loads current state
5. CLI compares current vs code-defined state
6. Changes applied with locking

## Key Abstractions

**Entity:**
- Business concept with join keys for feature linking
- Location: `src/strata/core.py:128`

**FeatureTable:**
- Container for features with transformation logic
- Location: `src/strata/core.py:13`

**Source Types:**
- Union: `SourceKind = BatchSource | StreamSource | RealTimeSource`
- Location: `src/strata/sources.py:17-36`

**Plugin Infrastructure:**
- Registry: `src/strata/plugins/sqlite/registry.py`
- Storage: `src/strata/plugins/local/storage.py`
- Compute: `src/strata/plugins/duckdb/compute.py`

## Entry Points

**CLI:**
- Location: `src/strata/__main__.py`
- Commands: new, preview, up, down, build

**Framework:**
- Location: `src/strata/__init__.py`
- Exports: FeatureTable, Feature, Dataset, Entity, Schema, Field, etc.

## Error Handling

**Strategy:** Pydantic validation with strict mode

**Patterns:**
- Model validators: `@pdt.model_validator(mode="after")`
- FileNotFoundError for missing config
- All plugin bases use `strict=True, frozen=True, extra="forbid"`

---

*Architecture analysis: 2026-02-02*
