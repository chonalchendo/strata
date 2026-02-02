# Coding Conventions

**Analysis Date:** 2026-02-02

## Naming Patterns

**Files:**
- Lowercase snake_case: `cli.py`, `settings.py`, `core.py`
- Plugin structure: `plugins/{provider}/{functionality}.py`

**Functions:**
- Lowercase snake_case: `load_strata_settings()`, `validate_target_exists()`
- CLI commands as verbs: `new()`, `preview()`, `up()`, `down()`, `build()`

**Variables:**
- Lowercase snake_case: `current_state`, `timestamp_field`
- Trailing underscore for reserved words: `schema_` (alias "schema")

**Types:**
- PascalCase: `StrataBaseModel`, `FeatureTable`, `BaseRegistry`
- Union types with pipe: `SourceKind = BatchSource | StreamSource | RealTimeSource`

## Code Style

**Import Organization:**
1. Python standard library
2. Third-party libraries (with aliases)
3. Local strata modules

**Example:**
```python
from functools import cache
from pathlib import Path

import omegaconf as oc
import pydantic as pdt
import pydantic_settings as pdts

import strata.plugins.factory as factory
```

**Type Aliasing:**
- `pdt` for `pydantic`
- `pdts` for `pydantic_settings`
- `oc` for `omegaconf`

**Linting:**
- Tool: `ruff>=0.14.14`

## Error Handling

**Patterns:**
- Pydantic validators: `@pdt.model_validator(mode="after")`
- Descriptive ValueError: `raise ValueError(f"Target '{self.target}' not found...")`
- FileNotFoundError for missing config
- Strict mode: `strict=True, extra="forbid"` on base classes

## Logging

**Framework:** Loguru (dependency, not yet integrated)

**Current:** Rich library for terminal output (`rich.print()`)

## Function Design

**Parameters:**
- Type hints required: `def load_strata_settings(path: Path = Path("strata.yaml")) -> StrataSettings:`
- CLI parameters with Annotated: `env: Annotated[str, cyclopts.Parameter(...)]`

**Return Values:**
- Type hints on all returns
- Property methods with `@property` decorator

## Module Design

**Exports:**
- Explicit `__all__` in `__init__.py` files
- Barrel file pattern in plugin packages

## Pydantic Configuration

**BaseModel inheritance:**
- All models inherit from `StrataBaseModel(pdt.BaseModel)`
- Settings use `pdts.BaseSettings` with strict validation
- Frozen models: `frozen=True` prevents modification
- Extra rejection: `extra="forbid"` prevents unknown fields

**Discriminated Unions:**
- Plugin selection via `discriminator="kind"` field
- Example: `kind: Literal["sqlite"] = "sqlite"`

---

*Convention analysis: 2026-02-02*
