"""Tests for CLI developer experience improvements.

Tests --json flag, structured JSON errors, verbose mode, and progress indicators.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pytest

import strata.cli as cli_mod
import strata.errors as errors
import strata.output as output_mod
from strata.cli import app


def run_cli(args: list[str]) -> None:
    """Run CLI command, catching successful exit."""
    try:
        app(args)
    except SystemExit as e:
        if e.code != 0:
            raise


# ---------------------------------------------------------------------------
# StrataError.to_dict()
# ---------------------------------------------------------------------------


class TestStrataErrorToDict:
    def test_strata_error_to_dict(self):
        """StrataError.to_dict() should return correct structure."""
        err = errors.StrataError(
            context="Loading config",
            cause="File not found",
            fix="Create strata.yaml",
        )
        result = err.to_dict()

        assert result["error"] is True
        assert result["code"] == "StrataError"
        assert result["context"] == "Loading config"
        assert result["cause"] == "File not found"
        assert result["fix"] == "Create strata.yaml"

    def test_config_not_found_error_code(self):
        """ConfigNotFoundError.to_dict() should have correct error code."""
        err = errors.ConfigNotFoundError(path="/path/to/strata.yaml")
        result = err.to_dict()

        assert result["error"] is True
        assert result["code"] == "ConfigNotFoundError"
        assert "strata.yaml" in result["context"]
        assert "not found" in result["cause"]
        assert result["fix"] is not None

    def test_build_error_code(self):
        """BuildError.to_dict() should have correct error code."""
        err = errors.BuildError(
            context="Building table 'users'",
            cause="Source data missing",
            fix="Run strata up first",
        )
        result = err.to_dict()

        assert result["code"] == "BuildError"

    def test_environment_not_found_error_code(self):
        """EnvironmentNotFoundError.to_dict() includes available envs."""
        err = errors.EnvironmentNotFoundError(
            env="prod", available=["dev", "staging"]
        )
        result = err.to_dict()

        assert result["code"] == "EnvironmentNotFoundError"
        assert "prod" in result["context"]


# ---------------------------------------------------------------------------
# JSON error output via _handle_error
# ---------------------------------------------------------------------------


class TestJsonErrorOutput:
    def test_json_error_output(self):
        """_handle_error with json_mode should print valid JSON."""
        err = errors.StrataError(
            context="Validating config",
            cause="Invalid format",
            fix="Check the docs",
        )

        printed = []

        def capture_print(*args, **kwargs):
            printed.append(str(args[0]) if args else "")

        with patch.object(cli_mod.console, "print", capture_print):
            cli_mod._handle_error(err, json_mode=True)

        assert len(printed) == 1
        data = json.loads(printed[0])
        assert data["error"] is True
        assert data["code"] == "StrataError"
        assert data["context"] == "Validating config"
        assert data["cause"] == "Invalid format"
        assert data["fix"] == "Check the docs"

    def test_rich_error_output(self):
        """_handle_error without json_mode should print Rich formatted text."""
        err = errors.StrataError(
            context="Validating config",
            cause="Invalid format",
            fix="Check the docs",
        )

        printed = []

        def capture_print(*args, **kwargs):
            printed.append(str(args[0]) if args else "")

        with patch.object(cli_mod.console, "print", capture_print):
            cli_mod._handle_error(err, json_mode=False)

        output_str = " ".join(printed)
        assert "Error:" in output_str
        assert "Cause:" in output_str
        assert "Fix:" in output_str


# ---------------------------------------------------------------------------
# ls --json
# ---------------------------------------------------------------------------


class TestLsJsonOutput:
    def test_ls_json_output_produces_valid_json(self, tmp_path, monkeypatch):
        """ls --json should produce valid JSON array."""
        config = tmp_path / "strata.yaml"
        config.write_text(
            """
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
      catalog: features
"""
        )

        entities_dir = tmp_path / "entities"
        entities_dir.mkdir()
        (entities_dir / "user.py").write_text(
            """
import strata.core as core
user = core.Entity(name="user", join_keys=["user_id"])
"""
        )

        monkeypatch.chdir(tmp_path)

        # Register objects
        with patch.object(cli_mod.console, "print"):
            with patch.object(output_mod.console, "print"):
                run_cli(["up", "--yes"])

        # List with --json
        printed = []

        def capture_print(*args, **kwargs):
            printed.append(str(args[0]) if args else "")

        with patch.object(cli_mod.console, "print", capture_print):
            run_cli(["ls", "--json"])

        # Should produce valid JSON array
        json_str = printed[-1]
        data = json.loads(json_str)
        assert isinstance(data, list)
        assert len(data) >= 1

        # Each object has expected fields
        obj = data[0]
        assert "kind" in obj
        assert "name" in obj
        assert "version" in obj
        assert "hash" in obj

    def test_ls_json_empty_registry(self, tmp_path, monkeypatch):
        """ls --json with empty registry should return empty JSON array."""
        config = tmp_path / "strata.yaml"
        config.write_text(
            """
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
      catalog: features
"""
        )
        monkeypatch.chdir(tmp_path)

        printed = []

        def capture_print(*args, **kwargs):
            printed.append(str(args[0]) if args else "")

        with patch.object(cli_mod.console, "print", capture_print):
            run_cli(["ls", "--json"])

        json_str = printed[-1]
        data = json.loads(json_str)
        assert data == []


# ---------------------------------------------------------------------------
# validate --json
# ---------------------------------------------------------------------------


class TestValidateJsonOutput:
    def test_validate_json_output_format(self, tmp_path, monkeypatch):
        """validate --json should produce valid JSON with expected fields."""
        config = tmp_path / "strata.yaml"
        config.write_text(
            """
name: test-project
default_env: dev
schedules:
  - hourly
environments:
  dev:
    registry:
      kind: sqlite
      path: .strata/registry.db
    backend:
      kind: duckdb
      path: .strata/data
      catalog: features
"""
        )

        # Create entity (no duplicate issues)
        entities_dir = tmp_path / "entities"
        entities_dir.mkdir()
        (entities_dir / "user.py").write_text(
            """
import strata.core as core
user = core.Entity(name="user", join_keys=["user_id"])
"""
        )

        monkeypatch.chdir(tmp_path)

        printed = []

        def capture_print(*args, **kwargs):
            printed.append(str(args[0]) if args else "")

        with patch.object(cli_mod.console, "print", capture_print):
            run_cli(["validate", "--json"])

        # Find the JSON output
        json_str = printed[-1]
        data = json.loads(json_str)

        assert "passed" in data
        assert "has_warnings" in data
        assert "error_count" in data
        assert "warning_count" in data
        assert "issues" in data
        assert isinstance(data["issues"], list)
        assert data["passed"] is True

    def test_validate_json_with_errors(self, tmp_path, monkeypatch):
        """validate --json with validation errors should include them in issues."""
        config = tmp_path / "strata.yaml"
        config.write_text(
            """
name: test-project
default_env: dev
schedules:
  - hourly
environments:
  dev:
    registry:
      kind: sqlite
      path: .strata/registry.db
    backend:
      kind: duckdb
      path: .strata/data
      catalog: features
"""
        )

        tables_dir = tmp_path / "tables"
        tables_dir.mkdir()
        (tables_dir / "bad_table.py").write_text(
            """
import strata.core as core
import strata.sources as sources
from strata.backends.local.storage import LocalSourceConfig

user = core.Entity(name="user", join_keys=["user_id"])
source = sources.BatchSource(
    name="tx",
    config=LocalSourceConfig(path="./data.parquet"),
    timestamp_field="ts",
)
bad_table = core.FeatureTable(
    name="bad_table",
    source=source,
    entity=user,
    timestamp_field="ts",
    schedule="invalid_schedule",
)
"""
        )

        monkeypatch.chdir(tmp_path)

        printed = []

        def capture_print(*args, **kwargs):
            printed.append(str(args[0]) if args else "")

        with patch.object(cli_mod.console, "print", capture_print):
            with pytest.raises(SystemExit) as exc_info:
                app(["validate", "--json"])

        assert exc_info.value.code == 1

        json_str = printed[-1]
        data = json.loads(json_str)
        assert data["passed"] is False
        assert data["error_count"] > 0
        assert len(data["issues"]) > 0


# ---------------------------------------------------------------------------
# build --json
# ---------------------------------------------------------------------------


class TestBuildJsonOutput:
    def test_build_json_output_format(self, tmp_path, monkeypatch):
        """build --json should produce valid JSON with table results."""
        config = tmp_path / "strata.yaml"
        config.write_text(
            """
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
      catalog: features
"""
        )
        monkeypatch.chdir(tmp_path)

        import strata.build as build_mod

        mock_table_result = MagicMock()
        mock_table_result.table_name = "users"
        mock_table_result.status = build_mod.BuildStatus.SUCCESS
        mock_table_result.duration_ms = 150.0
        mock_table_result.row_count = 1000
        mock_table_result.error = None
        mock_table_result.validation_passed = True
        mock_table_result.validation_warnings = 0

        mock_result = MagicMock()
        mock_result.is_success = True
        mock_result.table_results = [mock_table_result]
        mock_result.success_count = 1
        mock_result.failed_count = 0
        mock_result.skipped_count = 0
        mock_result.validation_count = 1
        mock_result.validation_warning_count = 0

        mock_ft = MagicMock()
        mock_ft.name = "users"
        mock_ft.schedule = None
        discovered = [MagicMock(kind="feature_table", obj=mock_ft)]

        printed = []

        def capture_print(*args, **kwargs):
            printed.append(str(args[0]) if args else "")

        with patch(
            "strata.discovery.discover_definitions", return_value=discovered
        ):
            with patch("strata.build.BuildEngine") as mock_engine_cls:
                mock_engine_cls.return_value.build.return_value = mock_result
                with patch.object(cli_mod.console, "print", capture_print):
                    with patch.object(cli_mod.console, "status"):
                        run_cli(["build", "--json"])

        # Find JSON output
        json_str = printed[-1]
        data = json.loads(json_str)

        assert data["success"] is True
        assert "elapsed_seconds" in data
        assert data["success_count"] == 1
        assert data["failed_count"] == 0
        assert isinstance(data["tables"], list)
        assert len(data["tables"]) == 1
        assert data["tables"][0]["table"] == "users"
        assert data["tables"][0]["status"] == "success"
        assert data["tables"][0]["row_count"] == 1000


# ---------------------------------------------------------------------------
# Verbose mode smoke test
# ---------------------------------------------------------------------------


class TestVerboseMode:
    def test_verbose_mode_does_not_crash(self, tmp_path, monkeypatch):
        """-v flag should not crash the command."""
        config = tmp_path / "strata.yaml"
        config.write_text(
            """
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
      catalog: features
"""
        )
        monkeypatch.chdir(tmp_path)

        printed = []

        def capture_print(*args, **kwargs):
            printed.append(str(args))

        with patch.object(cli_mod.console, "print", capture_print):
            run_cli(["validate", "-v"])

    def test_build_verbose_flag_accepted(self, capsys):
        """build --help should show -v/--verbose flag."""
        with pytest.raises(SystemExit) as exc_info:
            app(["build", "--help"])

        assert exc_info.value.code == 0
        captured = capsys.readouterr()
        assert "--verbose" in captured.out or "-v" in captured.out

    def test_validate_verbose_flag_accepted(self, capsys):
        """validate --help should show -v/--verbose flag."""
        with pytest.raises(SystemExit) as exc_info:
            app(["validate", "--help"])

        assert exc_info.value.code == 0
        captured = capsys.readouterr()
        assert "--verbose" in captured.out or "-v" in captured.out

    def test_quality_verbose_flag_accepted(self, capsys):
        """quality --help should show -v/--verbose flag."""
        with pytest.raises(SystemExit) as exc_info:
            app(["quality", "--help"])

        assert exc_info.value.code == 0
        captured = capsys.readouterr()
        assert "--verbose" in captured.out or "-v" in captured.out

    def test_freshness_verbose_flag_accepted(self, capsys):
        """freshness --help should show -v/--verbose flag."""
        with pytest.raises(SystemExit) as exc_info:
            app(["freshness", "--help"])

        assert exc_info.value.code == 0
        captured = capsys.readouterr()
        assert "--verbose" in captured.out or "-v" in captured.out


# ---------------------------------------------------------------------------
# --json flag presence on commands
# ---------------------------------------------------------------------------


class TestJsonFlagPresence:
    def test_build_help_shows_json_flag(self, capsys):
        """build --help should show --json flag."""
        with pytest.raises(SystemExit) as exc_info:
            app(["build", "--help"])

        assert exc_info.value.code == 0
        captured = capsys.readouterr()
        assert "--json" in captured.out

    def test_ls_help_shows_json_flag(self, capsys):
        """ls --help should show --json flag."""
        with pytest.raises(SystemExit) as exc_info:
            app(["ls", "--help"])

        assert exc_info.value.code == 0
        captured = capsys.readouterr()
        assert "--json" in captured.out

    def test_validate_help_shows_json_flag(self, capsys):
        """validate --help should show --json flag."""
        with pytest.raises(SystemExit) as exc_info:
            app(["validate", "--help"])

        assert exc_info.value.code == 0
        captured = capsys.readouterr()
        assert "--json" in captured.out
