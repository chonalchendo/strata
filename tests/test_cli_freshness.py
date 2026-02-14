"""Tests for strata freshness CLI command."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import pytest

import strata.cli as cli_mod
import strata.freshness as freshness_mod
import strata.registry as reg_types
from strata.cli import app


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


def _mock_discovered_tables(names: list[str], slas: list | None = None):
    """Create mock discovered objects with feature tables."""
    discovered = []
    for i, name in enumerate(names):
        mock_ft = MagicMock()
        mock_ft.name = name
        mock_ft.sla = slas[i] if slas else None
        discovered.append(MagicMock(kind="feature_table", obj=mock_ft))
    return discovered


def _mock_registry_with_builds(builds: dict[str, reg_types.BuildRecord | None]):
    """Create a mock registry that returns specified build records."""
    mock_reg = MagicMock()
    mock_reg.get_latest_build = lambda name: builds.get(name)
    return mock_reg


class TestFreshnessCommandBasic:
    def test_freshness_command_runs(self, project_dir, monkeypatch):
        """freshness command should run without error when tables exist."""
        monkeypatch.chdir(project_dir)

        discovered = _mock_discovered_tables(["users"])
        build = reg_types.BuildRecord(
            id=1,
            timestamp=datetime(2025, 1, 1, 11, 0, 0, tzinfo=timezone.utc),
            table_name="users",
            status="success",
            row_count=1000,
        )

        mock_result = freshness_mod.FreshnessResult(
            tables=[
                freshness_mod.TableFreshness(
                    table_name="users",
                    last_build_at=build.timestamp,
                    data_timestamp_max=None,
                    build_staleness=timedelta(hours=1),
                    data_staleness=None,
                    max_staleness=None,
                    status="fresh",
                    severity="warn",
                    row_count=1000,
                )
            ],
            has_stale=False,
            has_unknown=False,
        )

        with patch(
            "strata.discovery.discover_definitions", return_value=discovered
        ):
            with patch.object(cli_mod, "_get_registry") as mock_get_reg:
                mock_reg = MagicMock()
                mock_reg.get_latest_build.return_value = build
                mock_get_reg.return_value = mock_reg

                with patch(
                    "strata.freshness.check_freshness", return_value=mock_result
                ):
                    with patch.object(cli_mod.console, "print"):
                        run_cli(["freshness"])


class TestFreshnessJsonOutput:
    def test_freshness_json_output_format(self, project_dir, monkeypatch):
        """freshness --json should output valid JSON."""
        monkeypatch.chdir(project_dir)

        import json as json_lib

        discovered = _mock_discovered_tables(["users"])
        build = reg_types.BuildRecord(
            id=1,
            timestamp=datetime(2025, 1, 1, 11, 0, 0, tzinfo=timezone.utc),
            table_name="users",
            status="success",
            row_count=1000,
        )

        mock_result = freshness_mod.FreshnessResult(
            tables=[
                freshness_mod.TableFreshness(
                    table_name="users",
                    last_build_at=build.timestamp,
                    data_timestamp_max=None,
                    build_staleness=timedelta(hours=1),
                    data_staleness=None,
                    max_staleness=timedelta(hours=6),
                    status="fresh",
                    severity="warn",
                    row_count=1000,
                )
            ],
            has_stale=False,
            has_unknown=False,
        )

        printed = []

        def capture_print(*args, **kwargs):
            printed.append(str(args[0]) if args else "")

        with patch(
            "strata.discovery.discover_definitions", return_value=discovered
        ):
            with patch.object(cli_mod, "_get_registry") as mock_get_reg:
                mock_reg = MagicMock()
                mock_reg.get_latest_build.return_value = build
                mock_get_reg.return_value = mock_reg

                with patch(
                    "strata.freshness.check_freshness", return_value=mock_result
                ):
                    with patch.object(cli_mod.console, "print", capture_print):
                        run_cli(["freshness", "--json"])

        # Should have printed valid JSON
        json_output = printed[-1]
        parsed = json_lib.loads(json_output)
        assert "tables" in parsed
        assert "has_stale" in parsed
        assert "has_unknown" in parsed
        assert len(parsed["tables"]) == 1
        assert parsed["tables"][0]["table"] == "users"
        assert parsed["tables"][0]["status"] == "fresh"


class TestFreshnessExitCodeOnError:
    def test_exit_code_1_on_error_severity_staleness(
        self, project_dir, monkeypatch
    ):
        """freshness should exit 1 when any table has status='error'."""
        monkeypatch.chdir(project_dir)

        discovered = _mock_discovered_tables(["users"])
        build = reg_types.BuildRecord(
            id=1,
            timestamp=datetime(2025, 1, 1, 4, 0, 0, tzinfo=timezone.utc),
            table_name="users",
            status="success",
        )

        mock_result = freshness_mod.FreshnessResult(
            tables=[
                freshness_mod.TableFreshness(
                    table_name="users",
                    last_build_at=build.timestamp,
                    data_timestamp_max=None,
                    build_staleness=timedelta(hours=8),
                    data_staleness=None,
                    max_staleness=timedelta(hours=6),
                    status="error",
                    severity="error",
                )
            ],
            has_stale=True,
            has_unknown=False,
        )

        with patch(
            "strata.discovery.discover_definitions", return_value=discovered
        ):
            with patch.object(cli_mod, "_get_registry") as mock_get_reg:
                mock_reg = MagicMock()
                mock_reg.get_latest_build.return_value = build
                mock_get_reg.return_value = mock_reg

                with patch(
                    "strata.freshness.check_freshness", return_value=mock_result
                ):
                    with patch.object(cli_mod.console, "print"):
                        with pytest.raises(SystemExit) as exc_info:
                            app(["freshness"])

        assert exc_info.value.code == 1

    def test_exit_code_0_on_warn_severity(self, project_dir, monkeypatch):
        """freshness should exit 0 when tables have warn but not error status."""
        monkeypatch.chdir(project_dir)

        discovered = _mock_discovered_tables(["users"])
        build = reg_types.BuildRecord(
            id=1,
            timestamp=datetime(2025, 1, 1, 4, 0, 0, tzinfo=timezone.utc),
            table_name="users",
            status="success",
        )

        mock_result = freshness_mod.FreshnessResult(
            tables=[
                freshness_mod.TableFreshness(
                    table_name="users",
                    last_build_at=build.timestamp,
                    data_timestamp_max=None,
                    build_staleness=timedelta(hours=8),
                    data_staleness=None,
                    max_staleness=timedelta(hours=6),
                    status="warn",
                    severity="warn",
                )
            ],
            has_stale=True,
            has_unknown=False,
        )

        with patch(
            "strata.discovery.discover_definitions", return_value=discovered
        ):
            with patch.object(cli_mod, "_get_registry") as mock_get_reg:
                mock_reg = MagicMock()
                mock_reg.get_latest_build.return_value = build
                mock_get_reg.return_value = mock_reg

                with patch(
                    "strata.freshness.check_freshness", return_value=mock_result
                ):
                    with patch.object(cli_mod.console, "print"):
                        run_cli(["freshness"])
                        # Should NOT raise SystemExit -- warn is not error
