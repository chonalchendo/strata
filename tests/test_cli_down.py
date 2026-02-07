"""Tests for strata down command."""

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


class TestDownCommand:
    """Test strata down command."""

    def test_down_removes_all_objects(self, project_with_objects, monkeypatch):
        """down --yes should remove all objects."""
        monkeypatch.chdir(project_with_objects)

        # First, register objects
        with patch.object(cli_mod.console, "print"):
            with patch.object(output_mod.console, "print"):
                run_cli(["up", "--yes"])

        # Verify objects exist
        from strata.backends.sqlite.registry import SqliteRegistry

        registry_path = project_with_objects / ".strata" / "registry.db"
        reg = SqliteRegistry(kind="sqlite", path=str(registry_path))
        reg.initialize()
        assert len(reg.list_objects()) == 2

        # Remove all
        with patch.object(cli_mod.console, "print"):
            run_cli(["down", "--yes"])

        # Verify empty
        assert len(reg.list_objects()) == 0

    def test_down_specific_object(self, project_with_objects, monkeypatch):
        """down <kind> <name> should remove specific object."""
        monkeypatch.chdir(project_with_objects)

        # First, register objects
        with patch.object(cli_mod.console, "print"):
            with patch.object(output_mod.console, "print"):
                run_cli(["up", "--yes"])

        from strata.backends.sqlite.registry import SqliteRegistry

        registry_path = project_with_objects / ".strata" / "registry.db"
        reg = SqliteRegistry(kind="sqlite", path=str(registry_path))
        reg.initialize()
        assert len(reg.list_objects()) == 2

        # Remove specific entity
        with patch.object(cli_mod.console, "print"):
            run_cli(["down", "entity", "user", "--yes"])

        # Verify only one object remains
        objects = reg.list_objects()
        assert len(objects) == 1
        assert objects[0].name == "product"

    def test_down_invalid_kind_errors(self, project_with_objects, monkeypatch):
        """down with invalid kind should error."""
        monkeypatch.chdir(project_with_objects)

        with patch.object(cli_mod.console, "print"):
            with patch.object(output_mod.console, "print"):
                run_cli(["up", "--yes"])

        with patch.object(cli_mod.console, "print"):
            with pytest.raises(SystemExit) as exc_info:
                run_cli(["down", "invalid_kind", "foo", "--yes"])

        assert exc_info.value.code == 1

    def test_down_nonexistent_object_no_error(self, project_with_objects, monkeypatch):
        """down nonexistent object should not error, just warn."""
        monkeypatch.chdir(project_with_objects)

        with patch.object(cli_mod.console, "print"):
            with patch.object(output_mod.console, "print"):
                run_cli(["up", "--yes"])

        # This should not raise - it just prints "Object not found"
        with patch.object(cli_mod.console, "print"):
            run_cli(["down", "entity", "nonexistent", "--yes"])

    def test_down_requires_both_kind_and_name(self, project_with_objects, monkeypatch):
        """down with only kind should error."""
        monkeypatch.chdir(project_with_objects)

        with patch.object(cli_mod.console, "print"):
            with patch.object(output_mod.console, "print"):
                run_cli(["up", "--yes"])

        with patch.object(cli_mod.console, "print"):
            with pytest.raises(SystemExit) as exc_info:
                run_cli(["down", "entity", "--yes"])

        assert exc_info.value.code == 1

    def test_down_empty_registry(self, project_with_objects, monkeypatch):
        """down on empty registry should show no objects message."""
        monkeypatch.chdir(project_with_objects)

        printed = []

        def capture_print(*args, **kwargs):
            printed.append(str(args))

        with patch.object(cli_mod.console, "print", capture_print):
            run_cli(["down", "--yes"])

        output_str = " ".join(printed)
        assert "no objects" in output_str.lower()
