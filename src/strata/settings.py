"""Configuration loading and validation for Strata projects.

Configuration is loaded from strata.yaml and validated using Pydantic.
Supports environment-specific settings with catalog injection.
"""

from __future__ import annotations

from functools import cache
from pathlib import Path

import omegaconf as oc
import pydantic as pdt
import pydantic_settings as pdts

import strata.errors as errors
import strata.plugins.factory as factory


class Settings(pdts.BaseSettings, strict=True, frozen=True, extra="forbid"):
    """Base settings class with strict validation."""

    pass


class PathsSettings(Settings):
    """Project path configuration.

    Defines where feature definitions are located within the project.
    All paths are relative to the project root (where strata.yaml lives).
    """

    tables: str = "tables/"
    datasets: str = "datasets/"
    entities: str = "entities/"


class EnvironmentSettings(Settings):
    """Configuration for a single environment (dev, stg, prd).

    Each environment specifies its own registry, storage, and compute backends.
    The catalog field enables environment-specific catalog injection for sources.
    """

    catalog: str | None = None
    registry: factory.RegistryKind = pdt.Field(..., discriminator="kind")
    storage: factory.StorageKind = pdt.Field(..., discriminator="kind")
    compute: factory.ComputeKind = pdt.Field(..., discriminator="kind")


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
    paths: PathsSettings = pdt.Field(default_factory=PathsSettings)
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
