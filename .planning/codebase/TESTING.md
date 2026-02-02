# Testing Patterns

**Analysis Date:** 2026-02-02

## Test Framework

**Runner:** pytest>=9.0.2

**Run Commands:**
```bash
pytest                 # Run all tests
pytest -v              # Verbose output
pytest --cov           # Coverage report
pytest -k test_name    # Run specific tests
```

## Test File Organization

**Location:** No test files exist yet

**Recommended Structure:**
```
tests/
├── __init__.py
├── test_core.py           # Core models
├── test_settings.py       # Configuration
├── test_cli.py            # CLI commands
├── plugins/
│   ├── test_registry.py
│   ├── test_storage.py
│   └── test_compute.py
└── fixtures/
    └── conftest.py        # Shared fixtures
```

**Naming:**
- Files: `test_*.py` or `*_test.py`
- Functions: `test_function_name()`
- Classes: `class TestClassName:`

## Mocking

**What to Mock:**
- File I/O: `pathlib.Path.exists()`, `OmegaConf.load()`
- External APIs: registry state, storage operations
- Plugin implementations when testing factory

**What NOT to Mock:**
- Pydantic model validation
- Type system
- Python language features

## Fixtures

**Location:** `tests/conftest.py`

**Example:**
```python
@pytest.fixture
def city_entity():
    return Entity(
        name="City",
        description="City entity",
        join_keys=["city"]
    )
```

## Coverage

**Target:** 80%+ on core, 60%+ on plugins

**View:**
```bash
pytest --cov=src/strata --cov-report=html
```

## Test Types

**Unit Tests:**
- Individual functions, validators
- Pydantic models, properties

**Integration Tests:**
- Multiple components together
- Config loading with real files

## Error Testing

```python
def test_missing_required_field():
    with pytest.raises(ValidationError):
        FeatureTable(name="test")  # Missing required fields

def test_missing_config_file(tmp_path):
    with pytest.raises(FileNotFoundError):
        load_strata_settings(tmp_path / "missing.yaml")
```

## Test Priorities

**High (Core Logic):**
1. Settings loading and validation
2. Core model initialization
3. Plugin factory discriminator logic
4. Pydantic validators

**Medium (CLI & Integration):**
1. CLI parameter parsing
2. Registry state management
3. Configuration resolution

**Low (External):**
1. Plugin implementations
2. External API integrations

---

*Testing analysis: 2026-02-02*
