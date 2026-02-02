"""Tests for structured error handling."""

from __future__ import annotations

import strata.errors as errors


class TestStrataError:
    """Tests for base error class."""

    def test_error_has_context_cause_fix(self) -> None:
        """Error contains context, cause, and fix."""
        err = errors.StrataError(
            context="Loading configuration",
            cause="File not found",
            fix="Create the file",
        )

        assert err.context == "Loading configuration"
        assert err.cause == "File not found"
        assert err.fix == "Create the file"

    def test_error_message_format(self) -> None:
        """Error message combines all parts."""
        err = errors.StrataError(
            context="Loading configuration",
            cause="File not found",
            fix="Create the file",
        )

        message = str(err)
        assert "Loading configuration" in message
        assert "File not found" in message
        assert "Create the file" in message
        assert "Cause:" in message
        assert "Fix:" in message


class TestConfigurationErrors:
    """Tests for configuration error classes."""

    def test_config_not_found_error(self) -> None:
        """ConfigNotFoundError has correct message."""
        err = errors.ConfigNotFoundError("/path/to/strata.yaml")

        assert "/path/to/strata.yaml" in str(err)
        assert "not found" in str(err).lower()
        assert "strata new" in str(err)

    def test_config_validation_error(self) -> None:
        """ConfigValidationError has correct message."""
        err = errors.ConfigValidationError(
            path="/path/to/strata.yaml",
            details="missing required field: name",
        )

        assert "/path/to/strata.yaml" in str(err)
        assert "missing required field: name" in str(err)

    def test_environment_not_found_error(self) -> None:
        """EnvironmentNotFoundError lists available environments."""
        err = errors.EnvironmentNotFoundError(
            env="prod",
            available=["dev", "stg"],
        )

        assert "prod" in str(err)
        assert "dev" in str(err)
        assert "stg" in str(err)

    def test_environment_not_found_error_empty_available(self) -> None:
        """EnvironmentNotFoundError handles empty available list."""
        err = errors.EnvironmentNotFoundError(
            env="dev",
            available=[],
        )

        assert "dev" in str(err)
        assert "(none)" in str(err)

    def test_invalid_schedule_error(self) -> None:
        """InvalidScheduleError lists allowed schedules."""
        err = errors.InvalidScheduleError(
            schedule="monthly",
            allowed=["hourly", "daily"],
        )

        assert "monthly" in str(err)
        assert "hourly" in str(err)
        assert "daily" in str(err)

    def test_invalid_schedule_error_empty_allowed(self) -> None:
        """InvalidScheduleError handles empty allowed list."""
        err = errors.InvalidScheduleError(
            schedule="daily",
            allowed=[],
        )

        assert "daily" in str(err)
        assert "(none defined)" in str(err)


class TestErrorInheritance:
    """Tests for error class hierarchy."""

    def test_configuration_error_is_strata_error(self) -> None:
        """ConfigurationError inherits from StrataError."""
        err = errors.ConfigNotFoundError("test.yaml")
        assert isinstance(err, errors.StrataError)
        assert isinstance(err, errors.ConfigurationError)

    def test_all_errors_are_exceptions(self) -> None:
        """All error classes are exceptions."""
        err = errors.StrataError("ctx", "cause", "fix")
        assert isinstance(err, Exception)
