"""Configuration loading and validation for Strata projects.

Configuration is loaded from strata.yaml and validated using Pydantic.
Supports environment-specific settings with catalog injection.
"""

from __future__ import annotations

from functools import cache
from pathlib import Path
from typing import Annotated, ClassVar, Union

import omegaconf as oc
import pydantic as pdt
import pydantic_settings as pdts

import strata.errors as errors
import strata.plugins as plugins


class Settings(pdts.BaseSettings, strict=True, frozen=True, extra="forbid"):
    """Base settings class with strict validation."""

    pass


class LegacyPathsSettings(Settings):
    """Legacy path configuration (backward compatible).

    Defines where feature definitions are located within the project.
    All paths are relative to the project root (where strata.yaml lives).
    """

    tables: str = "tables/"
    datasets: str = "datasets/"
    entities: str = "entities/"


class SmartPathsSettings(Settings):
    """Smart path configuration with include/exclude patterns.

    When no paths section is provided, or when using include/exclude,
    Strata will scan all Python files and use isinstance() to find SDK objects.

    Default exclusions skip test files, virtual environments, and other
    non-source directories.
    """

    include: list[str] = pdt.Field(default_factory=list)
    exclude: list[str] = pdt.Field(default_factory=list)

    DEFAULT_EXCLUDES: ClassVar[list[str]] = [
        "test_*.py",
        "*_test.py",
        "conftest.py",
        "**/tests/**",
        "**/test/**",
        "**/__pycache__/**",
        "**/.*/**",  # Hidden directories
        "**/venv/**",
        "**/.venv/**",
        "**/env/**",
        "**/node_modules/**",
        "**/build/**",
        "**/dist/**",
        "**/*.egg-info/**",
        "**/site-packages/**",
    ]


def _discriminate_paths(v: dict | LegacyPathsSettings | SmartPathsSettings) -> str:
    """Discriminate between legacy and smart paths configuration."""
    if isinstance(v, SmartPathsSettings):
        return "smart"
    if isinstance(v, LegacyPathsSettings):
        return "legacy"

    keys = set(v.keys())
    has_legacy = bool(keys & {"tables", "datasets", "entities"})
    has_smart = bool(keys & {"include", "exclude"})

    if has_legacy and has_smart:
        msg = "Cannot mix legacy paths (tables/datasets/entities) with smart paths (include/exclude)"
        raise ValueError(msg)

    return "legacy" if has_legacy else "smart"


PathsSettings = Annotated[
    Union[
        Annotated[LegacyPathsSettings, pdt.Tag("legacy")],
        Annotated[SmartPathsSettings, pdt.Tag("smart")],
    ],
    pdt.Discriminator(_discriminate_paths),
]


class EnvironmentSettings(Settings):
    """Configuration for a single environment (dev, stg, prd).

    Each environment specifies its own registry, storage, and compute backends.
    The catalog field enables environment-specific catalog injection for sources.
    """

    catalog: str | None = None
    registry: plugins.RegistryKind = pdt.Field(..., discriminator="kind")
    storage: plugins.StorageKind = pdt.Field(..., discriminator="kind")
    compute: plugins.ComputeKind = pdt.Field(..., discriminator="kind")


class StrataSettings(Settings):
    """Root configuration loaded from strata.yaml.

    Example strata.yaml:
        name: my-feature-store
        default_env: dev
        schedules:
          - hourly
          - daily
        paths:
          tables: tables/
          datasets: datasets/
          entities: entities/
        environments:
          dev:
            catalog: my_catalog_dev
            registry:
              kind: sqlite
              path: .strata/registry.db
            storage:
              kind: local
              path: .strata/data
              catalog: features
            compute:
              kind: duckdb
    """

    name: str
    default_env: str
    schedules: list[str] = pdt.Field(default_factory=list)
    paths: PathsSettings = pdt.Field(default_factory=SmartPathsSettings)
    environments: dict[str, EnvironmentSettings]

    # Internal: tracks which env is currently active (set via resolve_environment)
    _active_env: str | None = pdt.PrivateAttr(default=None)
    _config_path: Path | None = pdt.PrivateAttr(default=None)

    @pdt.model_validator(mode="after")
    def validate_default_env_exists(self) -> StrataSettings:
        """Ensure default_env references a defined environment."""
        if self.default_env not in self.environments:
            raise ValueError(
                f"default_env '{self.default_env}' not found in environments: "
                f"{list(self.environments.keys())}"
            )
        return self

    @property
    def active_env(self) -> str:
        """Get the currently active environment name."""
        return self._active_env or self.default_env

    @property
    def active_environment(self) -> EnvironmentSettings:
        """Get the environment config for the currently active environment."""
        return self.environments[self.active_env]

    def resolve_environment(self, env: str | None = None) -> StrataSettings:
        """Set the active environment, validating it exists.

        Args:
            env: Environment name to activate. If None, uses default_env.

        Returns:
            Self, for chaining.

        Raises:
            EnvironmentNotFoundError: If env is not defined.
        """
        target = env or self.default_env
        if target not in self.environments:
            raise errors.EnvironmentNotFoundError(
                env=target,
                available=list(self.environments.keys()),
            )
        object.__setattr__(self, "_active_env", target)
        return self

    def validate_schedule(self, schedule: str) -> None:
        """Validate a schedule tag against the allowed schedules list.

        Args:
            schedule: Schedule tag to validate (e.g., "hourly", "daily").

        Raises:
            InvalidScheduleError: If schedule is not in the allowed list.
        """
        if self.schedules and schedule not in self.schedules:
            raise errors.InvalidScheduleError(
                schedule=schedule,
                allowed=self.schedules,
            )


def load_strata_settings(
    path: Path | str = Path("strata.yaml"),
    env: str | None = None,
) -> StrataSettings:
    """Load and validate Strata configuration from a YAML file.

    Args:
        path: Path to strata.yaml file.
        env: Environment to activate. If None, uses default_env from config.

    Returns:
        Validated StrataSettings instance.

    Raises:
        ConfigNotFoundError: If config file doesn't exist.
        ConfigValidationError: If config fails validation.
    """
    path = Path(path)

    if not path.exists():
        raise errors.ConfigNotFoundError(str(path))

    try:
        config = oc.OmegaConf.load(path)
        config_dict = oc.OmegaConf.to_container(config, resolve=True)
        settings = StrataSettings.model_validate(config_dict)
        object.__setattr__(settings, "_config_path", path)
        settings.resolve_environment(env)
        return settings
    except pdt.ValidationError as e:
        raise errors.ConfigValidationError(
            path=str(path),
            details=_format_validation_errors(e),
        ) from e
    except oc.errors.OmegaConfBaseException as e:
        raise errors.ConfigValidationError(
            path=str(path),
            details=str(e),
        ) from e


def _format_validation_errors(error: pdt.ValidationError) -> str:
    """Format Pydantic validation errors into readable messages."""
    messages = []
    for err in error.errors():
        loc = ".".join(str(x) for x in err["loc"])
        msg = err["msg"]
        messages.append(f"  - {loc}: {msg}")
    return "\n".join(messages)


@cache
def get_settings() -> StrataSettings:
    """Get cached settings instance. Use for CLI commands.

    This is a convenience function that caches the loaded settings.
    For testing or when you need to load from a specific path,
    use load_strata_settings() directly.
    """
    return load_strata_settings()
