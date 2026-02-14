"""Tests for strata build command."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from strata.cli import app
import strata.cli as cli_mod


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
schedules:
  - hourly
  - daily
environments:
  dev:
    registry:
      kind: sqlite
      path: .strata/registry.db
    backend:
      kind: duckdb
      path: .strata/data
      catalog: features
  staging:
    registry:
      kind: sqlite
      path: .strata/staging-registry.db
    backend:
      kind: duckdb
      path: .strata/staging-data
      catalog: features_staging
"""
    )
    return tmp_path


def _mock_discovered_tables(schedules=None):
    """Create mock discovered objects with feature tables.

    Args:
        schedules: List of schedule values for each table, or None for a
            single unscheduled table.
    """
    if schedules is None:
        schedules = [None]

    discovered = []
    for i, sched in enumerate(schedules):
        mock_ft = MagicMock()
        mock_ft.name = f"table_{i}"
        mock_ft.schedule = sched
        discovered.append(MagicMock(kind="feature_table", obj=mock_ft))
    return discovered


def _mock_build_result(*, is_success=True, table_results=None):
    """Create a mock BuildResult."""
    result = MagicMock()
    result.is_success = is_success
    result.table_results = table_results or []
    result.success_count = sum(
        1 for t in result.table_results if getattr(t, "_success", True)
    )
    result.failed_count = 0 if is_success else 1
    result.skipped_count = 0
    result.validation_count = 0
    result.validation_warning_count = 0
    return result


# ---------------------------------------------------------------------------
# Help output
# ---------------------------------------------------------------------------


class TestBuildHelp:
    def test_build_help_shows_all_options(self, capsys):
        """build --help should show all flags."""
        with pytest.raises(SystemExit) as exc_info:
            app(["build", "--help"])

        # Cyclopts exits 0 on --help
        assert exc_info.value.code == 0

        captured = capsys.readouterr()
        # Check key options are present
        assert "--schedule" in captured.out
        assert "--full-refresh" in captured.out
        assert "--start" in captured.out
        assert "--end" in captured.out
        assert "--env" in captured.out
        assert "TABLE" in captured.out or "--table" in captured.out


# ---------------------------------------------------------------------------
# Backend sourced from settings
# ---------------------------------------------------------------------------


class TestBuildUsesSettingsBackend:
    def test_build_uses_env_backend(self, project_dir, monkeypatch):
        """Build should use backend from settings.active_environment, not manual construction."""
        monkeypatch.chdir(project_dir)

        discovered = _mock_discovered_tables()
        mock_result = _mock_build_result()

        with patch(
            "strata.discovery.discover_definitions", return_value=discovered
        ):
            with patch("strata.build.BuildEngine") as mock_engine_cls:
                mock_engine_cls.return_value.build.return_value = mock_result
                with patch.object(cli_mod.console, "print"):
                    run_cli(["build"])

                # BuildEngine should be constructed with the settings backend
                mock_engine_cls.assert_called_once()
                call_kwargs = mock_engine_cls.call_args
                assert "backend" in call_kwargs.kwargs


# ---------------------------------------------------------------------------
# Table targeting
# ---------------------------------------------------------------------------


class TestBuildSpecificTable:
    def test_build_passes_target(self, project_dir, monkeypatch):
        """build <table> should pass targets=[table] to engine.build()."""
        monkeypatch.chdir(project_dir)

        discovered = _mock_discovered_tables()
        mock_result = _mock_build_result()

        with patch(
            "strata.discovery.discover_definitions", return_value=discovered
        ):
            with patch("strata.build.BuildEngine") as mock_engine_cls:
                mock_engine_cls.return_value.build.return_value = mock_result
                with patch.object(cli_mod.console, "print"):
                    run_cli(["build", "my_table"])

                build_call = mock_engine_cls.return_value.build.call_args
                assert build_call.kwargs["targets"] == ["my_table"]

    def test_build_no_target_passes_none(self, project_dir, monkeypatch):
        """build without table name should pass targets=None."""
        monkeypatch.chdir(project_dir)

        discovered = _mock_discovered_tables()
        mock_result = _mock_build_result()

        with patch(
            "strata.discovery.discover_definitions", return_value=discovered
        ):
            with patch("strata.build.BuildEngine") as mock_engine_cls:
                mock_engine_cls.return_value.build.return_value = mock_result
                with patch.object(cli_mod.console, "print"):
                    run_cli(["build"])

                build_call = mock_engine_cls.return_value.build.call_args
                assert build_call.kwargs["targets"] is None


# ---------------------------------------------------------------------------
# Full refresh flag
# ---------------------------------------------------------------------------


class TestBuildFullRefresh:
    def test_full_refresh_passed_to_engine(self, project_dir, monkeypatch):
        """--full-refresh should pass full_refresh=True to engine.build()."""
        monkeypatch.chdir(project_dir)

        discovered = _mock_discovered_tables()
        mock_result = _mock_build_result()

        with patch(
            "strata.discovery.discover_definitions", return_value=discovered
        ):
            with patch("strata.build.BuildEngine") as mock_engine_cls:
                mock_engine_cls.return_value.build.return_value = mock_result
                with patch.object(cli_mod.console, "print"):
                    run_cli(["build", "--full-refresh"])

                build_call = mock_engine_cls.return_value.build.call_args
                assert build_call.kwargs["full_refresh"] is True

    def test_no_full_refresh_is_false(self, project_dir, monkeypatch):
        """Without --full-refresh, full_refresh should be False."""
        monkeypatch.chdir(project_dir)

        discovered = _mock_discovered_tables()
        mock_result = _mock_build_result()

        with patch(
            "strata.discovery.discover_definitions", return_value=discovered
        ):
            with patch("strata.build.BuildEngine") as mock_engine_cls:
                mock_engine_cls.return_value.build.return_value = mock_result
                with patch.object(cli_mod.console, "print"):
                    run_cli(["build"])

                build_call = mock_engine_cls.return_value.build.call_args
                assert build_call.kwargs["full_refresh"] is False


# ---------------------------------------------------------------------------
# Date range (start/end)
# ---------------------------------------------------------------------------


class TestBuildDateRange:
    def test_start_end_passed_to_engine(self, project_dir, monkeypatch):
        """--start and --end should parse dates and pass to engine.build()."""
        monkeypatch.chdir(project_dir)

        discovered = _mock_discovered_tables()
        mock_result = _mock_build_result()

        with patch(
            "strata.discovery.discover_definitions", return_value=discovered
        ):
            with patch("strata.build.BuildEngine") as mock_engine_cls:
                mock_engine_cls.return_value.build.return_value = mock_result
                with patch.object(cli_mod.console, "print"):
                    run_cli(
                        [
                            "build",
                            "--start",
                            "2024-01-01",
                            "--end",
                            "2024-02-01",
                        ]
                    )

                build_call = mock_engine_cls.return_value.build.call_args
                from datetime import datetime

                assert build_call.kwargs["start"] == datetime(2024, 1, 1)
                assert build_call.kwargs["end"] == datetime(2024, 2, 1)

    def test_start_without_end_errors(self, project_dir, monkeypatch):
        """--start without --end should exit non-zero."""
        monkeypatch.chdir(project_dir)

        with patch.object(cli_mod.console, "print"):
            with pytest.raises(SystemExit) as exc_info:
                app(["build", "--start", "2024-01-01"])

        assert exc_info.value.code == 1

    def test_end_without_start_errors(self, project_dir, monkeypatch):
        """--end without --start should exit non-zero."""
        monkeypatch.chdir(project_dir)

        with patch.object(cli_mod.console, "print"):
            with pytest.raises(SystemExit) as exc_info:
                app(["build", "--end", "2024-02-01"])

        assert exc_info.value.code == 1

    def test_no_dates_passes_none(self, project_dir, monkeypatch):
        """Without --start/--end, start and end should be None."""
        monkeypatch.chdir(project_dir)

        discovered = _mock_discovered_tables()
        mock_result = _mock_build_result()

        with patch(
            "strata.discovery.discover_definitions", return_value=discovered
        ):
            with patch("strata.build.BuildEngine") as mock_engine_cls:
                mock_engine_cls.return_value.build.return_value = mock_result
                with patch.object(cli_mod.console, "print"):
                    run_cli(["build"])

                build_call = mock_engine_cls.return_value.build.call_args
                assert build_call.kwargs["start"] is None
                assert build_call.kwargs["end"] is None


# ---------------------------------------------------------------------------
# Build failure
# ---------------------------------------------------------------------------


class TestBuildFailedExitsNonzero:
    def test_failed_build_exits_nonzero(self, project_dir, monkeypatch):
        """Build failure should exit with code 1."""
        monkeypatch.chdir(project_dir)

        import strata.build as build_mod

        failed_result = MagicMock()
        failed_result.is_success = False
        failed_result.table_results = [
            MagicMock(
                table_name="broken_table",
                status=build_mod.BuildStatus.FAILED,
                error="Something went wrong",
                duration_ms=100.0,
                row_count=None,
                validation_passed=None,
                validation_warnings=0,
            ),
        ]
        failed_result.success_count = 0
        failed_result.failed_count = 1
        failed_result.skipped_count = 0
        failed_result.validation_count = 0
        failed_result.validation_warning_count = 0

        discovered = _mock_discovered_tables()

        with patch(
            "strata.discovery.discover_definitions", return_value=discovered
        ):
            with patch("strata.build.BuildEngine") as mock_engine_cls:
                mock_engine_cls.return_value.build.return_value = failed_result
                with patch.object(cli_mod.console, "print"):
                    with pytest.raises(SystemExit) as exc_info:
                        app(["build"])

        assert exc_info.value.code == 1


# ---------------------------------------------------------------------------
# Environment flag
# ---------------------------------------------------------------------------


class TestBuildEnvFlag:
    def test_env_passed_to_settings(self, project_dir, monkeypatch):
        """--env should select which environment's backend to use."""
        monkeypatch.chdir(project_dir)

        mock_settings = MagicMock()
        mock_settings.active_env = "staging"
        mock_settings.active_environment.backend = MagicMock()
        mock_settings.schedules = []

        with patch(
            "strata.settings.load_strata_settings", return_value=mock_settings
        ) as mock_load:
            with patch(
                "strata.discovery.discover_definitions", return_value=[]
            ):
                with patch.object(cli_mod.console, "print"):
                    run_cli(["build", "--env", "staging"])

            mock_load.assert_called_once_with(env="staging")


# ---------------------------------------------------------------------------
# Schedule filter
# ---------------------------------------------------------------------------


class TestBuildScheduleFilter:
    def test_schedule_filters_tables(self, project_dir, monkeypatch):
        """--schedule should filter tables by schedule tag."""
        monkeypatch.chdir(project_dir)

        discovered = _mock_discovered_tables(
            schedules=["hourly", "daily", None]
        )

        mock_result = _mock_build_result()

        with patch(
            "strata.discovery.discover_definitions", return_value=discovered
        ):
            with patch("strata.build.BuildEngine") as mock_engine_cls:
                mock_engine_cls.return_value.build.return_value = mock_result
                with patch.object(cli_mod.console, "print"):
                    run_cli(["build", "--schedule", "hourly"])

                # Only the hourly table should be built
                build_call = mock_engine_cls.return_value.build.call_args
                tables_passed = build_call.kwargs["tables"]
                assert len(tables_passed) == 1
                assert tables_passed[0].schedule == "hourly"

    def test_schedule_no_matching_tables(self, project_dir, monkeypatch):
        """--schedule with no matching tables should exit gracefully."""
        monkeypatch.chdir(project_dir)

        discovered = _mock_discovered_tables(schedules=["daily"])

        printed = []

        def capture_print(*args, **kwargs):
            printed.append(str(args))

        with patch(
            "strata.discovery.discover_definitions", return_value=discovered
        ):
            with patch.object(cli_mod.console, "print", capture_print):
                run_cli(["build", "--schedule", "hourly"])

        output_str = " ".join(printed)
        assert "hourly" in output_str

    def test_invalid_schedule_errors(self, project_dir, monkeypatch):
        """--schedule with invalid tag should error via settings validation."""
        monkeypatch.chdir(project_dir)

        # The project has schedules: [hourly, daily] so "weekly" should fail
        with patch.object(cli_mod.console, "print"):
            with pytest.raises(SystemExit) as exc_info:
                app(["build", "--schedule", "weekly"])

        assert exc_info.value.code == 1


# ---------------------------------------------------------------------------
# --skip-quality flag
# ---------------------------------------------------------------------------


class TestBuildSkipQualityFlag:
    def test_build_skip_quality_flag(self, project_dir, monkeypatch):
        """--skip-quality should be accepted and passed to engine.build()."""
        monkeypatch.chdir(project_dir)

        discovered = _mock_discovered_tables()
        mock_result = _mock_build_result()

        with patch(
            "strata.discovery.discover_definitions", return_value=discovered
        ):
            with patch("strata.build.BuildEngine") as mock_engine_cls:
                mock_engine_cls.return_value.build.return_value = mock_result
                with patch.object(cli_mod.console, "print"):
                    run_cli(["build", "--skip-quality"])

                build_call = mock_engine_cls.return_value.build.call_args
                assert build_call.kwargs["skip_quality"] is True

    def test_build_no_skip_quality_defaults_false(
        self, project_dir, monkeypatch
    ):
        """Without --skip-quality, skip_quality should be False."""
        monkeypatch.chdir(project_dir)

        discovered = _mock_discovered_tables()
        mock_result = _mock_build_result()

        with patch(
            "strata.discovery.discover_definitions", return_value=discovered
        ):
            with patch("strata.build.BuildEngine") as mock_engine_cls:
                mock_engine_cls.return_value.build.return_value = mock_result
                with patch.object(cli_mod.console, "print"):
                    run_cli(["build"])

                build_call = mock_engine_cls.return_value.build.call_args
                assert build_call.kwargs["skip_quality"] is False

    def test_build_help_shows_skip_quality(self, capsys):
        """build --help should show --skip-quality flag."""
        with pytest.raises(SystemExit) as exc_info:
            app(["build", "--help"])

        assert exc_info.value.code == 0

        captured = capsys.readouterr()
        assert "--skip-quality" in captured.out
