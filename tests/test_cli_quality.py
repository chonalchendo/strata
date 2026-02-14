"""Tests for strata quality command."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

import strata.cli as cli_mod
from strata.cli import app
import strata.quality as quality_mod
import strata.registry as reg_types


def run_cli(args: list[str]) -> None:
    """Run CLI command, catching successful exit."""
    try:
        app(args)
    except SystemExit as e:
        if e.code != 0:
            raise


@pytest.fixture
def project_dir(tmp_path):
    """Create a minimal project directory with strata.yaml."""
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
    return tmp_path


def _make_validation_result(
    *,
    table_name: str = "user_features",
    passed: bool = True,
    has_warnings: bool = False,
    rows_checked: int = 1000,
    constraints: list[quality_mod.ConstraintResult] | None = None,
) -> quality_mod.TableValidationResult:
    """Build a TableValidationResult for tests."""
    if constraints is None:
        constraints = [
            quality_mod.ConstraintResult(
                field_name="amount",
                constraint="ge",
                passed=True,
                severity="error",
                expected=">= 0",
                actual="min=5.0",
                rows_checked=rows_checked,
                rows_failed=0,
            ),
        ]

    field_results = []
    fields_seen: dict[str, list[quality_mod.ConstraintResult]] = {}
    for cr in constraints:
        fields_seen.setdefault(cr.field_name, []).append(cr)

    for fname, crs in fields_seen.items():
        error_passed = all(c.passed for c in crs if c.severity == "error")
        field_results.append(
            quality_mod.FieldResult(
                field_name=fname,
                constraints=crs,
                passed=error_passed,
            )
        )

    return quality_mod.TableValidationResult(
        table_name=table_name,
        field_results=field_results,
        rows_checked=rows_checked,
        passed=passed,
        has_warnings=has_warnings,
    )


def _make_quality_record(
    result: quality_mod.TableValidationResult,
) -> reg_types.QualityResultRecord:
    """Build a QualityResultRecord from a TableValidationResult."""
    from dataclasses import asdict

    return reg_types.QualityResultRecord(
        id=1,
        timestamp=datetime.now(timezone.utc),
        table_name=result.table_name,
        passed=result.passed,
        has_warnings=result.has_warnings,
        rows_checked=result.rows_checked,
        results_json=json.dumps(asdict(result), default=str),
    )


# ---------------------------------------------------------------------------
# No results
# ---------------------------------------------------------------------------


class TestQualityNoResults:
    def test_quality_no_results_shows_hint(self, project_dir, monkeypatch):
        """When no quality results exist, show hint message."""
        monkeypatch.chdir(project_dir)

        mock_reg = MagicMock()
        mock_reg.get_quality_results.return_value = []

        mock_settings = MagicMock()
        mock_settings.active_environment.registry = mock_reg

        printed = []

        def capture_print(*args, **kwargs):
            printed.append(str(args))

        with patch(
            "strata.settings.load_strata_settings", return_value=mock_settings
        ):
            with patch.object(cli_mod.console, "print", capture_print):
                run_cli(["quality", "user_features"])

        output_str = " ".join(printed)
        assert "No quality results found" in output_str
        assert (
            "strata build" in output_str
            or "strata quality --live" in output_str
        )


# ---------------------------------------------------------------------------
# With results
# ---------------------------------------------------------------------------


class TestQualityWithResults:
    def test_quality_with_results_renders_table(self, project_dir, monkeypatch):
        """When quality results exist, render Rich table."""
        monkeypatch.chdir(project_dir)

        result = _make_validation_result()
        record = _make_quality_record(result)

        mock_reg = MagicMock()
        mock_reg.get_quality_results.return_value = [record]

        mock_settings = MagicMock()
        mock_settings.active_environment.registry = mock_reg

        printed = []

        def capture_print(*args, **kwargs):
            printed.append(str(args))

        with patch(
            "strata.settings.load_strata_settings", return_value=mock_settings
        ):
            with patch.object(cli_mod.console, "print", capture_print):
                run_cli(["quality", "user_features"])

        output_str = " ".join(printed)
        assert "Quality: user_features" in output_str
        assert "PASSED" in output_str
        assert "1000" in output_str


# ---------------------------------------------------------------------------
# JSON output
# ---------------------------------------------------------------------------


class TestQualityJsonOutput:
    def test_quality_json_output_format(self, project_dir, monkeypatch):
        """--json should output valid JSON with expected fields."""
        monkeypatch.chdir(project_dir)

        result = _make_validation_result()
        record = _make_quality_record(result)

        mock_reg = MagicMock()
        mock_reg.get_quality_results.return_value = [record]

        mock_settings = MagicMock()
        mock_settings.active_environment.registry = mock_reg

        printed = []

        def capture_print(*args, **kwargs):
            printed.append(str(args[0]) if args else "")

        with patch(
            "strata.settings.load_strata_settings", return_value=mock_settings
        ):
            with patch.object(cli_mod.console, "print", capture_print):
                run_cli(["quality", "user_features", "--json"])

        # Find the JSON output (the last print call should be the JSON)
        json_str = printed[-1]
        data = json.loads(json_str)

        assert data["table"] == "user_features"
        assert data["passed"] is True
        assert data["has_warnings"] is False
        assert data["rows_checked"] == 1000
        assert isinstance(data["fields"], list)
        assert len(data["fields"]) == 1
        assert data["fields"][0]["field"] == "amount"
        assert data["fields"][0]["constraints"][0]["constraint"] == "ge"


# ---------------------------------------------------------------------------
# Exit code on failure
# ---------------------------------------------------------------------------


class TestQualityExitCodeOnFailure:
    def test_quality_exit_code_on_error_failure(self, project_dir, monkeypatch):
        """Exit code should be 1 when error-severity constraint fails."""
        monkeypatch.chdir(project_dir)

        result = _make_validation_result(
            passed=False,
            constraints=[
                quality_mod.ConstraintResult(
                    field_name="amount",
                    constraint="ge",
                    passed=False,
                    severity="error",
                    expected=">= 0",
                    actual="min=-5.0",
                    rows_checked=1000,
                    rows_failed=10,
                ),
            ],
        )
        record = _make_quality_record(result)

        mock_reg = MagicMock()
        mock_reg.get_quality_results.return_value = [record]

        mock_settings = MagicMock()
        mock_settings.active_environment.registry = mock_reg

        with patch(
            "strata.settings.load_strata_settings", return_value=mock_settings
        ):
            with patch.object(cli_mod.console, "print"):
                with pytest.raises(SystemExit) as exc_info:
                    app(["quality", "user_features"])

        assert exc_info.value.code == 1


# ---------------------------------------------------------------------------
# Warnings still pass
# ---------------------------------------------------------------------------


class TestQualityWarningsStillPass:
    def test_warn_severity_does_not_exit_nonzero(
        self, project_dir, monkeypatch
    ):
        """Warn-severity failures should NOT cause exit code 1."""
        monkeypatch.chdir(project_dir)

        result = _make_validation_result(
            passed=True,
            has_warnings=True,
            constraints=[
                quality_mod.ConstraintResult(
                    field_name="amount",
                    constraint="ge",
                    passed=False,
                    severity="warn",
                    expected=">= 0",
                    actual="min=-5.0",
                    rows_checked=1000,
                    rows_failed=10,
                ),
            ],
        )
        record = _make_quality_record(result)

        mock_reg = MagicMock()
        mock_reg.get_quality_results.return_value = [record]

        mock_settings = MagicMock()
        mock_settings.active_environment.registry = mock_reg

        with patch(
            "strata.settings.load_strata_settings", return_value=mock_settings
        ):
            with patch.object(cli_mod.console, "print"):
                # Should NOT raise SystemExit
                run_cli(["quality", "user_features"])


# ---------------------------------------------------------------------------
# Table not found (live mode)
# ---------------------------------------------------------------------------


class TestQualityTableNotFound:
    def test_quality_live_table_not_found(self, project_dir, monkeypatch):
        """--live with non-existent table should exit 1 with error message."""
        monkeypatch.chdir(project_dir)

        mock_reg = MagicMock()

        mock_settings = MagicMock()
        mock_settings.active_environment.registry = mock_reg

        # No feature tables discovered
        with patch(
            "strata.settings.load_strata_settings", return_value=mock_settings
        ):
            with patch(
                "strata.discovery.discover_definitions", return_value=[]
            ):
                with patch.object(cli_mod.console, "print"):
                    with pytest.raises(SystemExit) as exc_info:
                        app(["quality", "nonexistent", "--live"])

        assert exc_info.value.code == 1


# ---------------------------------------------------------------------------
# Help output
# ---------------------------------------------------------------------------


class TestQualityHelp:
    def test_quality_help_shows_options(self, capsys):
        """quality --help should show all flags."""
        with pytest.raises(SystemExit) as exc_info:
            app(["quality", "--help"])

        assert exc_info.value.code == 0

        captured = capsys.readouterr()
        assert "--json" in captured.out
        assert "--live" in captured.out
        assert "--env" in captured.out
        assert "TABLE" in captured.out or "--table" in captured.out


# ---------------------------------------------------------------------------
# JSON output on failure
# ---------------------------------------------------------------------------


class TestQualityJsonExitCode:
    def test_json_output_exits_on_failure(self, project_dir, monkeypatch):
        """--json with failing constraints should still exit 1."""
        monkeypatch.chdir(project_dir)

        result = _make_validation_result(
            passed=False,
            constraints=[
                quality_mod.ConstraintResult(
                    field_name="score",
                    constraint="le",
                    passed=False,
                    severity="error",
                    expected="<= 100",
                    actual="max=150",
                    rows_checked=500,
                    rows_failed=3,
                ),
            ],
        )
        record = _make_quality_record(result)

        mock_reg = MagicMock()
        mock_reg.get_quality_results.return_value = [record]

        mock_settings = MagicMock()
        mock_settings.active_environment.registry = mock_reg

        printed = []

        def capture_print(*args, **kwargs):
            printed.append(str(args[0]) if args else "")

        with patch(
            "strata.settings.load_strata_settings", return_value=mock_settings
        ):
            with patch.object(cli_mod.console, "print", capture_print):
                with pytest.raises(SystemExit) as exc_info:
                    app(["quality", "user_features", "--json"])

        assert exc_info.value.code == 1

        # Should still produce valid JSON before exiting
        json_str = printed[-1]
        data = json.loads(json_str)
        assert data["passed"] is False
