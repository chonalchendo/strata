"""Structured error handling with context + cause + fix pattern.

All Strata errors follow a consistent pattern that provides:
- Context: What operation was being attempted
- Cause: Why it failed
- Fix: How to resolve the issue

This makes errors actionable and reduces debugging time.
"""

from __future__ import annotations


class StrataError(Exception):
    """Base error with structured messaging.

    All Strata errors inherit from this class and provide
    context, cause, and fix information.
    """

    def __init__(self, context: str, cause: str, fix: str) -> None:
        self.context = context
        self.cause = cause
        self.fix = fix
        message = f"{context}\n\nCause: {cause}\n\nFix: {fix}"
        super().__init__(message)

    def to_dict(self) -> dict:
        """Serialize error to structured dict for JSON output."""
        return {
            "error": True,
            "code": type(self).__name__,
            "context": self.context,
            "cause": self.cause,
            "fix": self.fix,
        }


class ConfigurationError(StrataError):
    """Configuration file or settings related errors."""

    pass


class ConfigNotFoundError(ConfigurationError):
    """Configuration file not found."""

    def __init__(self, path: str) -> None:
        super().__init__(
            context=f"Loading configuration from '{path}'",
            cause="Configuration file not found",
            fix=f"Create a strata.yaml file at '{path}' or run 'strata new' to initialize a project",
        )


class ConfigValidationError(ConfigurationError):
    """Configuration validation failed."""

    def __init__(self, path: str, details: str) -> None:
        super().__init__(
            context=f"Validating configuration from '{path}'",
            cause=details,
            fix="Check the configuration file matches the expected schema. See documentation for strata.yaml format.",
        )


class EnvironmentNotFoundError(ConfigurationError):
    """Requested environment not defined in configuration."""

    def __init__(self, env: str, available: list[str]) -> None:
        available_str = ", ".join(available) if available else "(none)"
        super().__init__(
            context=f"Resolving environment '{env}'",
            cause=f"Environment '{env}' is not defined in strata.yaml",
            fix=f"Use one of the available environments: {available_str}, or add '{env}' to the environments section",
        )


class InvalidScheduleError(ConfigurationError):
    """Schedule tag not in allowed schedules list."""

    def __init__(self, schedule: str, allowed: list[str]) -> None:
        allowed_str = ", ".join(allowed) if allowed else "(none defined)"
        super().__init__(
            context=f"Validating schedule tag '{schedule}'",
            cause=f"Schedule '{schedule}' is not in the allowed schedules list",
            fix=f"Use one of the allowed schedules: {allowed_str}, or add '{schedule}' to the schedules list in strata.yaml",
        )


class RegistryError(StrataError):
    """Registry operation errors."""

    pass


class StorageError(StrataError):
    """Storage operation errors."""

    pass


class BuildError(StrataError):
    """Build/materialization errors."""

    pass
