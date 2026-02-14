"""Tests for strata ls command."""

from unittest.mock import patch

import pytest

from strata.cli import app
import strata.cli as cli_mod
import strata.output as output_mod


def run_cli(args: list[str]) -> None:
    """Run CLI command, catching successful exit."""
    try:
        app(args)
    except SystemExit as e:
        if e.code != 0:
            raise


@pytest.fixture
def project_with_objects(tmp_path):
    """Create a project with registered objects."""
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

    # Create entities directory with definitions
    entities_dir = tmp_path / "entities"
    entities_dir.mkdir()
    (entities_dir / "user.py").write_text(
        """
import strata.core as core
user = core.Entity(name="user", join_keys=["user_id"])
"""
    )
    (entities_dir / "product.py").write_text(
        """
import strata.core as core
product = core.Entity(name="product", join_keys=["product_id"])
"""
    )

    return tmp_path


class TestLsCommand:
    """Test strata ls command."""

    def test_ls_shows_all_objects(self, project_with_objects, monkeypatch):
        """ls should show all registered objects."""
        monkeypatch.chdir(project_with_objects)

        # Register objects
        with patch.object(cli_mod.console, "print"):
            with patch.object(output_mod.console, "print"):
                run_cli(["up", "--yes"])

        # List objects - check via registry directly
        from strata.infra.backends.sqlite.registry import SqliteRegistry

        registry_path = project_with_objects / ".strata" / "registry.db"
        reg = SqliteRegistry(kind="sqlite", path=str(registry_path))
        reg.initialize()

        objects = reg.list_objects()
        assert len(objects) == 2
        names = [o.name for o in objects]
        assert "user" in names
        assert "product" in names

        # Also verify ls doesn't error
        with patch.object(cli_mod.console, "print"):
            run_cli(["ls"])

    def test_ls_filters_by_kind(self, project_with_objects, monkeypatch):
        """ls <kind> should filter to specific kind."""
        monkeypatch.chdir(project_with_objects)

        # Register objects
        with patch.object(cli_mod.console, "print"):
            with patch.object(output_mod.console, "print"):
                run_cli(["up", "--yes"])

        # Verify via registry that filtering by kind works
        from strata.infra.backends.sqlite.registry import SqliteRegistry

        registry_path = project_with_objects / ".strata" / "registry.db"
        reg = SqliteRegistry(kind="sqlite", path=str(registry_path))
        reg.initialize()

        # Only entities registered
        entities = reg.list_objects(kind="entity")
        assert len(entities) == 2

        # No feature tables
        feature_tables = reg.list_objects(kind="feature_table")
        assert len(feature_tables) == 0

        # Verify ls entity doesn't error
        with patch.object(cli_mod.console, "print"):
            run_cli(["ls", "entity"])

    def test_ls_invalid_kind_errors(self, project_with_objects, monkeypatch):
        """ls with invalid kind should error."""
        monkeypatch.chdir(project_with_objects)

        with patch.object(cli_mod.console, "print"):
            with patch.object(output_mod.console, "print"):
                run_cli(["up", "--yes"])

        with patch.object(cli_mod.console, "print"):
            with pytest.raises(SystemExit) as exc_info:
                run_cli(["ls", "invalid_kind"])

        assert exc_info.value.code == 1

    def test_ls_empty_registry(self, project_with_objects, monkeypatch):
        """ls on empty registry should show no objects message."""
        monkeypatch.chdir(project_with_objects)

        printed = []

        def capture_print(*args, **kwargs):
            printed.append(str(args))

        with patch.object(cli_mod.console, "print", capture_print):
            run_cli(["ls"])

        output_str = " ".join(printed)
        assert "no objects" in output_str.lower()

    def test_ls_empty_kind_filter(self, project_with_objects, monkeypatch):
        """ls <kind> with no objects of that kind shows specific message."""
        monkeypatch.chdir(project_with_objects)

        # Register objects
        with patch.object(cli_mod.console, "print"):
            with patch.object(output_mod.console, "print"):
                run_cli(["up", "--yes"])

        # List feature_tables (none exist)
        printed = []

        def capture_print(*args, **kwargs):
            printed.append(str(args))

        with patch.object(cli_mod.console, "print", capture_print):
            run_cli(["ls", "feature_table"])

        output_str = " ".join(printed)
        assert "no feature_table" in output_str.lower()


class TestCompileSourceTableRejection:
    """Test that compile rejects SourceTables."""

    def test_compile_rejects_source_table(self, tmp_path, monkeypatch):
        """compile <source_table> should error with helpful message."""
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

        # Create a source table
        tables_dir = tmp_path / "tables"
        tables_dir.mkdir()
        (tables_dir / "events.py").write_text(
            """
import strata.core as core
import strata.sources as sources
import strata.infra.backends.local.storage as local_storage

entity = core.Entity(name="user", join_keys=["user_id"])

source = sources.BatchSource(
    name="events_source",
    config=local_storage.LocalSourceConfig(path="data/events.csv", format="csv"),
    timestamp_field="ts",
)

class EventsSchema(core.Schema):
    user_id = core.Field(dtype="string")
    ts = core.Field(dtype="datetime")
    event = core.Field(dtype="string")

events_st = core.SourceTable(
    name="events",
    source=source,
    entity=entity,
    timestamp_field="ts",
    schema=EventsSchema,
)
"""
        )

        monkeypatch.chdir(tmp_path)

        printed = []

        def capture_print(*args, **kwargs):
            printed.append(str(args))

        with patch.object(cli_mod.console, "print", capture_print):
            with pytest.raises(SystemExit) as exc_info:
                run_cli(["compile", "events"])

        assert exc_info.value.code == 1
        output_str = " ".join(printed)
        assert "SourceTable" in output_str
        assert "FeatureTable" in output_str
