"""Tests for strata publish command and build --publish flag."""

from __future__ import annotations

import json
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pytest

import strata.cli as cli_mod
import strata.core as core
import strata.sources as sources
from strata.cli import app


def run_cli(args: list[str]) -> None:
    """Run CLI command, catching successful exit."""
    try:
        app(args)
    except SystemExit as e:
        if e.code != 0:
            raise


def _make_entity() -> core.Entity:
    return core.Entity(name="user", join_keys=["user_id"])


def _make_feature_table(
    name: str = "user_features",
    *,
    online: bool = True,
) -> core.FeatureTable:
    from strata.infra.backends.local import LocalSourceConfig

    source = sources.BatchSource(
        name="events",
        config=LocalSourceConfig(path="./data/events.parquet"),
        timestamp_field="event_ts",
    )
    return core.FeatureTable(
        name=name,
        source=source,
        entity=_make_entity(),
        timestamp_field="event_ts",
        online=online,
    )


def _make_discovered(feature_tables: list) -> list:
    """Build mock discovered objects list from feature tables."""
    discovered = []
    for ft in feature_tables:
        mock_disc = MagicMock()
        mock_disc.kind = "feature_table"
        mock_disc.name = ft.name
        mock_disc.obj = ft
        discovered.append(mock_disc)
    return discovered


# ---------------------------------------------------------------------------
# Tests: publish -- no online store
# ---------------------------------------------------------------------------


class TestPublishNoOnlineStore:
    def test_publish_no_online_store(self, monkeypatch, tmp_path):
        """Publish command errors when no online store is configured."""
        monkeypatch.chdir(tmp_path)

        ft = _make_feature_table(online=True)
        discovered = _make_discovered([ft])

        mock_settings = MagicMock()
        mock_settings.active_environment.online_store = None
        mock_settings.active_environment.backend = MagicMock()

        printed = []

        def capture_print(*args, **kwargs):
            printed.append(str(args[0]) if args else "")

        with (
            patch(
                "strata.settings.load_strata_settings",
                return_value=mock_settings,
            ),
            patch(
                "strata.discovery.discover_definitions", return_value=discovered
            ),
            patch.object(cli_mod.console, "print", capture_print),
        ):
            with pytest.raises(SystemExit) as exc_info:
                app(["publish"])

        assert exc_info.value.code == 1
        output_str = " ".join(printed)
        assert "No online store configured" in output_str


# ---------------------------------------------------------------------------
# Tests: publish -- no online tables
# ---------------------------------------------------------------------------


class TestPublishNoOnlineTables:
    def test_publish_no_online_tables(self, monkeypatch, tmp_path):
        """Publish shows message when no tables have online=True."""
        monkeypatch.chdir(tmp_path)

        ft = _make_feature_table(online=False)
        discovered = _make_discovered([ft])

        mock_online = MagicMock()
        mock_settings = MagicMock()
        mock_settings.active_environment.online_store = mock_online
        mock_settings.active_environment.backend = MagicMock()

        printed = []

        def capture_print(*args, **kwargs):
            printed.append(str(args[0]) if args else "")

        with (
            patch(
                "strata.settings.load_strata_settings",
                return_value=mock_settings,
            ),
            patch(
                "strata.discovery.discover_definitions", return_value=discovered
            ),
            patch.object(cli_mod.console, "print", capture_print),
        ):
            run_cli(["publish"])

        output_str = " ".join(printed)
        assert "No online tables found" in output_str


# ---------------------------------------------------------------------------
# Tests: publish -- specific table
# ---------------------------------------------------------------------------


class TestPublishSpecificTable:
    def test_publish_specific_table(self, monkeypatch, tmp_path):
        """Publish a specific table by name."""
        monkeypatch.chdir(tmp_path)

        ft = _make_feature_table("user_features", online=True)
        discovered = _make_discovered([ft])

        mock_backend = MagicMock()
        mock_backend.table_exists.return_value = True
        mock_backend.read_table.return_value = pa.table(
            {
                "user_id": ["u1", "u2"],
                "spend": [100.0, 200.0],
                "event_ts": ["2024-01-01", "2024-01-02"],
            }
        )

        mock_online = MagicMock()
        mock_settings = MagicMock()
        mock_settings.active_environment.online_store = mock_online
        mock_settings.active_environment.backend = mock_backend

        printed = []

        def capture_print(*args, **kwargs):
            printed.append(str(args[0]) if args else "")

        with (
            patch(
                "strata.settings.load_strata_settings",
                return_value=mock_settings,
            ),
            patch(
                "strata.discovery.discover_definitions", return_value=discovered
            ),
            patch.object(cli_mod.console, "print", capture_print),
        ):
            run_cli(["publish", "user_features"])

        # Verify write_batch was called
        mock_online.write_batch.assert_called_once()
        call_kwargs = mock_online.write_batch.call_args
        assert call_kwargs[1]["table_name"] == "user_features"

        output_str = " ".join(printed)
        assert "Published 1 table" in output_str


# ---------------------------------------------------------------------------
# Tests: publish -- JSON output
# ---------------------------------------------------------------------------


class TestPublishJsonOutput:
    def test_publish_json_output(self, monkeypatch, tmp_path):
        """--json flag produces structured JSON output."""
        monkeypatch.chdir(tmp_path)

        ft = _make_feature_table("user_features", online=True)
        discovered = _make_discovered([ft])

        mock_backend = MagicMock()
        mock_backend.table_exists.return_value = True
        mock_backend.read_table.return_value = pa.table(
            {
                "user_id": ["u1", "u2", "u3"],
                "spend": [100.0, 200.0, 300.0],
                "event_ts": ["2024-01-01", "2024-01-02", "2024-01-03"],
            }
        )

        mock_online = MagicMock()
        mock_settings = MagicMock()
        mock_settings.active_environment.online_store = mock_online
        mock_settings.active_environment.backend = mock_backend

        printed = []

        def capture_print(*args, **kwargs):
            printed.append(str(args[0]) if args else "")

        with (
            patch(
                "strata.settings.load_strata_settings",
                return_value=mock_settings,
            ),
            patch(
                "strata.discovery.discover_definitions", return_value=discovered
            ),
            patch.object(cli_mod.console, "print", capture_print),
        ):
            run_cli(["publish", "--json"])

        # Find the JSON output
        json_str = printed[-1]
        data = json.loads(json_str)

        assert data["published"] == 1
        assert isinstance(data["tables"], list)
        assert len(data["tables"]) == 1
        assert data["tables"][0]["table"] == "user_features"
        assert data["tables"][0]["status"] == "published"
        assert data["tables"][0]["entities"] == 3


# ---------------------------------------------------------------------------
# Tests: build --publish
# ---------------------------------------------------------------------------


class TestBuildWithPublishFlag:
    def test_build_with_publish_flag(self, monkeypatch, tmp_path):
        """Build with --publish runs build then publish."""
        monkeypatch.chdir(tmp_path)

        ft = _make_feature_table("user_features", online=True)
        discovered = _make_discovered([ft])

        mock_backend = MagicMock()
        mock_backend.table_exists.return_value = True
        mock_backend.read_table.return_value = pa.table(
            {
                "user_id": ["u1"],
                "spend": [100.0],
                "event_ts": ["2024-01-01"],
            }
        )

        # Mock build engine result
        mock_build_result = MagicMock()
        mock_build_result.is_success = True
        mock_build_result.table_results = []
        mock_build_result.success_count = 1
        mock_build_result.failed_count = 0
        mock_build_result.skipped_count = 0
        mock_build_result.validation_count = 0
        mock_build_result.validation_warning_count = 0

        mock_engine = MagicMock()
        mock_engine.build.return_value = mock_build_result

        mock_online = MagicMock()
        mock_settings = MagicMock()
        mock_settings.active_environment.online_store = mock_online
        mock_settings.active_environment.backend = mock_backend
        mock_settings.active_environment.registry = MagicMock()
        mock_settings.active_env = "dev"

        printed = []

        def capture_print(*args, **kwargs):
            printed.append(str(args[0]) if args else "")

        with (
            patch(
                "strata.settings.load_strata_settings",
                return_value=mock_settings,
            ),
            patch(
                "strata.discovery.discover_definitions", return_value=discovered
            ),
            patch("strata.build.BuildEngine", return_value=mock_engine),
            patch.object(cli_mod.console, "print", capture_print),
        ):
            run_cli(["build", "--publish"])

        # Build was called
        mock_engine.build.assert_called_once()
        # Publish was also called
        mock_online.write_batch.assert_called_once()

        output_str = " ".join(printed)
        assert "Published 1 table" in output_str


# ---------------------------------------------------------------------------
# Tests: publish -- help output
# ---------------------------------------------------------------------------


class TestPublishHelp:
    def test_publish_help_shows_options(self, capsys):
        """publish --help should show all flags."""
        with pytest.raises(SystemExit) as exc_info:
            app(["publish", "--help"])

        assert exc_info.value.code == 0

        captured = capsys.readouterr()
        assert "--json" in captured.out
        assert "--env" in captured.out


# ---------------------------------------------------------------------------
# Tests: publish -- table not found
# ---------------------------------------------------------------------------


class TestPublishTableNotFound:
    def test_publish_specific_table_not_found(self, monkeypatch, tmp_path):
        """Publishing a nonexistent table raises an error."""
        monkeypatch.chdir(tmp_path)

        ft = _make_feature_table("user_features", online=True)
        discovered = _make_discovered([ft])

        mock_online = MagicMock()
        mock_settings = MagicMock()
        mock_settings.active_environment.online_store = mock_online
        mock_settings.active_environment.backend = MagicMock()

        printed = []

        def capture_print(*args, **kwargs):
            printed.append(str(args[0]) if args else "")

        with (
            patch(
                "strata.settings.load_strata_settings",
                return_value=mock_settings,
            ),
            patch(
                "strata.discovery.discover_definitions", return_value=discovered
            ),
            patch.object(cli_mod.console, "print", capture_print),
        ):
            with pytest.raises(SystemExit) as exc_info:
                app(["publish", "nonexistent_table"])

        assert exc_info.value.code == 1
        output_str = " ".join(printed)
        assert "not found" in output_str
