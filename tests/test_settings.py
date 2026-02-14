"""Tests for configuration loading and validation."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

import strata.errors as errors
import strata.settings as settings


@pytest.fixture
def valid_config() -> str:
    """Minimal valid configuration."""
    return """
name: test-project
default_env: dev
environments:
  dev:
    registry:
      kind: sqlite
      path: .strata/registry.db
    backend:
      kind: duckdb
      path: .strata/data
      catalog: test_catalog
"""


@pytest.fixture
def full_config() -> str:
    """Full configuration with all options."""
    return """
name: test-project
default_env: dev
schedules:
  - hourly
  - daily
  - weekly
paths:
  tables: features/tables/
  datasets: features/datasets/
  entities: features/entities/
environments:
  dev:
    catalog: test_dev
    registry:
      kind: sqlite
      path: .strata/dev/registry.db
    backend:
      kind: duckdb
      path: .strata/dev/data
      catalog: features_dev
  prd:
    catalog: test_prd
    registry:
      kind: sqlite
      path: .strata/prd/registry.db
    backend:
      kind: duckdb
      path: .strata/prd/data
      catalog: features_prd
"""


class TestLoadStrataSettings:
    """Tests for load_strata_settings function."""

    def test_load_valid_config(self, valid_config: str) -> None:
        """Valid configuration loads successfully."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            f.write(valid_config)
            f.flush()
            config = settings.load_strata_settings(Path(f.name))

        assert config.name == "test-project"
        assert config.default_env == "dev"
        assert config.active_env == "dev"

    def test_load_full_config(self, full_config: str) -> None:
        """Full configuration with all options loads successfully."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            f.write(full_config)
            f.flush()
            config = settings.load_strata_settings(Path(f.name))

        assert config.name == "test-project"
        assert config.schedules == ["hourly", "daily", "weekly"]
        assert config.paths.tables == "features/tables/"
        assert config.paths.datasets == "features/datasets/"
        assert len(config.environments) == 2

    def test_missing_config_raises_error(self) -> None:
        """Missing configuration file raises ConfigNotFoundError."""
        with pytest.raises(errors.ConfigNotFoundError) as exc_info:
            settings.load_strata_settings(Path("nonexistent.yaml"))

        assert "Configuration file not found" in str(exc_info.value)

    def test_invalid_yaml_raises_validation_error(self) -> None:
        """Invalid YAML raises ConfigValidationError."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            f.write("name: test\ndefault_env: dev\n# missing environments")
            f.flush()
            with pytest.raises(errors.ConfigValidationError):
                settings.load_strata_settings(Path(f.name))

    def test_invalid_default_env_raises_error(self) -> None:
        """default_env not in environments raises error."""
        config_str = """
name: test
default_env: nonexistent
environments:
  dev:
    registry:
      kind: sqlite
      path: .strata/registry.db
    backend:
      kind: duckdb
      path: .strata/data
      catalog: test
"""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            f.write(config_str)
            f.flush()
            with pytest.raises(errors.ConfigValidationError) as exc_info:
                settings.load_strata_settings(Path(f.name))

        assert "default_env" in str(exc_info.value)


class TestEnvironmentResolution:
    """Tests for environment resolution."""

    def test_resolve_default_environment(self, full_config: str) -> None:
        """Default environment is resolved correctly."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            f.write(full_config)
            f.flush()
            config = settings.load_strata_settings(Path(f.name))

        assert config.active_env == "dev"
        assert config.active_environment.catalog == "test_dev"

    def test_resolve_specific_environment(self, full_config: str) -> None:
        """Specific environment can be resolved."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            f.write(full_config)
            f.flush()
            config = settings.load_strata_settings(Path(f.name), env="prd")

        assert config.active_env == "prd"
        assert config.active_environment.catalog == "test_prd"

    def test_resolve_nonexistent_environment_raises_error(
        self, full_config: str
    ) -> None:
        """Resolving nonexistent environment raises EnvironmentNotFoundError."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            f.write(full_config)
            f.flush()
            with pytest.raises(errors.EnvironmentNotFoundError) as exc_info:
                settings.load_strata_settings(Path(f.name), env="nonexistent")

        assert "nonexistent" in str(exc_info.value)
        assert "dev" in str(exc_info.value)  # Should list available envs


class TestScheduleValidation:
    """Tests for schedule tag validation."""

    def test_validate_valid_schedule(self, full_config: str) -> None:
        """Valid schedule tag passes validation."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            f.write(full_config)
            f.flush()
            config = settings.load_strata_settings(Path(f.name))

        # Should not raise
        config.validate_schedule("daily")
        config.validate_schedule("hourly")
        config.validate_schedule("weekly")

    def test_validate_invalid_schedule_raises_error(
        self, full_config: str
    ) -> None:
        """Invalid schedule tag raises InvalidScheduleError."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            f.write(full_config)
            f.flush()
            config = settings.load_strata_settings(Path(f.name))

        with pytest.raises(errors.InvalidScheduleError) as exc_info:
            config.validate_schedule("monthly")

        assert "monthly" in str(exc_info.value)
        assert "hourly" in str(exc_info.value)  # Should list allowed schedules

    def test_no_schedules_allows_any(self, valid_config: str) -> None:
        """When no schedules defined, any schedule is allowed."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            f.write(valid_config)
            f.flush()
            config = settings.load_strata_settings(Path(f.name))

        # Should not raise when schedules list is empty
        config.validate_schedule("anything")


class TestPathsConfiguration:
    """Tests for paths configuration."""

    def test_no_paths_uses_smart_discovery(self, valid_config: str) -> None:
        """No paths section uses SmartPathsSettings with defaults."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            f.write(valid_config)
            f.flush()
            config = settings.load_strata_settings(Path(f.name))

        assert isinstance(config.paths, settings.SmartPathsSettings)
        assert config.paths.include == []
        assert config.paths.exclude == []

    def test_legacy_paths_uses_legacy_settings(self, full_config: str) -> None:
        """Legacy paths (tables/datasets/entities) use LegacyPathsSettings."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            f.write(full_config)
            f.flush()
            config = settings.load_strata_settings(Path(f.name))

        assert isinstance(config.paths, settings.LegacyPathsSettings)
        assert config.paths.tables == "features/tables/"
        assert config.paths.datasets == "features/datasets/"
        assert config.paths.entities == "features/entities/"

    def test_smart_paths_with_include_exclude(self) -> None:
        """Smart paths with include/exclude use SmartPathsSettings."""
        config_str = """
name: test-project
default_env: dev
paths:
  include:
    - src/features/
  exclude:
    - "**/scratch/**"
environments:
  dev:
    registry:
      kind: sqlite
      path: .strata/registry.db
    backend:
      kind: duckdb
      path: .strata/data
      catalog: test_catalog
"""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            f.write(config_str)
            f.flush()
            config = settings.load_strata_settings(Path(f.name))

        assert isinstance(config.paths, settings.SmartPathsSettings)
        assert config.paths.include == ["src/features/"]
        assert config.paths.exclude == ["**/scratch/**"]

    def test_mixing_legacy_and_smart_paths_raises_error(self) -> None:
        """Mixing legacy and smart paths raises ConfigValidationError."""
        config_str = """
name: test-project
default_env: dev
paths:
  tables: tables/
  include:
    - src/
environments:
  dev:
    registry:
      kind: sqlite
      path: .strata/registry.db
    backend:
      kind: duckdb
      path: .strata/data
      catalog: test_catalog
"""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            f.write(config_str)
            f.flush()
            with pytest.raises(errors.ConfigValidationError) as exc_info:
                settings.load_strata_settings(Path(f.name))

        assert "mix" in str(exc_info.value).lower()

    def test_smart_paths_default_excludes(self) -> None:
        """SmartPathsSettings has sensible default exclusions."""
        # Check DEFAULT_EXCLUDES contains expected patterns
        defaults = settings.SmartPathsSettings.DEFAULT_EXCLUDES
        assert "test_*.py" in defaults
        assert "*_test.py" in defaults
        assert "conftest.py" in defaults
        assert "**/tests/**" in defaults
        assert "**/venv/**" in defaults
        assert "**/__pycache__/**" in defaults


class TestCatalogInjection:
    """Tests for catalog injection per environment."""

    def test_catalog_available_per_environment(self, full_config: str) -> None:
        """Each environment has its own catalog."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            f.write(full_config)
            f.flush()

            dev_config = settings.load_strata_settings(Path(f.name), env="dev")
            assert dev_config.active_environment.catalog == "test_dev"

            prd_config = settings.load_strata_settings(Path(f.name), env="prd")
            assert prd_config.active_environment.catalog == "test_prd"

    def test_catalog_optional(self, valid_config: str) -> None:
        """Catalog is optional."""
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".yaml", delete=False
        ) as f:
            f.write(valid_config)
            f.flush()
            config = settings.load_strata_settings(Path(f.name))

        assert config.active_environment.catalog is None
