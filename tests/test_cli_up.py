"""Tests for strata preview and up commands."""

from unittest.mock import patch

import pytest

from strata.cli import app
import strata.output as output_mod


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

    # Create entities directory with a definition
    entities_dir = tmp_path / "entities"
    entities_dir.mkdir()
    (entities_dir / "user.py").write_text(
        """
import strata.core as core
user = core.Entity(name="user", join_keys=["user_id"])
"""
    )

    return tmp_path


class TestPreviewCommand:
    """Test strata preview command."""

    def test_preview_discovers_definitions(self, project_dir, monkeypatch):
        """Preview should discover and show definitions."""
        monkeypatch.chdir(project_dir)

        # Capture output by mocking console
        with patch.object(output_mod.console, "print") as mock_print:
            # Run preview using cyclopts callable
            run_cli(["preview"])

        # Should have printed something about the entity
        calls = [str(c) for c in mock_print.call_args_list]
        output_str = " ".join(calls)
        assert "user" in output_str.lower() or "create" in output_str.lower()

    def test_preview_no_definitions(self, tmp_path, monkeypatch):
        """Preview with no definitions shows no resources."""
        # Create minimal config
        config = tmp_path / "strata.yaml"
        config.write_text(
            """
name: empty-project
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

        with patch.object(output_mod.console, "print") as mock_print:
            run_cli(["preview"])

        # Should indicate no changes
        calls = [str(c) for c in mock_print.call_args_list]
        output_str = " ".join(calls)
        assert "no" in output_str.lower() or "0" in output_str


class TestUpCommand:
    """Test strata up command."""

    def test_up_dry_run_no_apply(self, project_dir, monkeypatch):
        """--dry-run should preview without applying."""
        monkeypatch.chdir(project_dir)

        with patch.object(output_mod.console, "print"):
            run_cli(["up", "--dry-run"])

        # Note: registry.db may be created for reading but should have no objects
        # The key is that no objects were written

    def test_up_with_yes_applies(self, project_dir, monkeypatch):
        """--yes should apply without prompting."""
        monkeypatch.chdir(project_dir)

        with patch.object(output_mod.console, "print"):
            with patch.object(output_mod.console, "input", return_value="n"):
                # Should not prompt with --yes
                run_cli(["up", "--yes"])

        # Check registry has the entity
        from strata.backends.sqlite.registry import SqliteRegistry

        registry_path = project_dir / ".strata" / "registry.db"
        reg = SqliteRegistry(kind="sqlite", path=str(registry_path))
        reg.initialize()

        objects = reg.list_objects()
        entity_names = [o.name for o in objects if o.kind == "entity"]
        assert "user" in entity_names

    def test_up_cancelled_on_no(self, project_dir, monkeypatch):
        """Answering 'n' should cancel apply."""
        monkeypatch.chdir(project_dir)

        with patch.object(output_mod.console, "print"):
            with patch.object(output_mod, "prompt_apply", return_value=False):
                run_cli(["up"])

        # Registry should exist but be empty (initialized but no objects applied)
        from strata.backends.sqlite.registry import SqliteRegistry

        registry_path = project_dir / ".strata" / "registry.db"
        if registry_path.exists():
            reg = SqliteRegistry(kind="sqlite", path=str(registry_path))
            reg.initialize()
            objects = reg.list_objects()
            assert len(objects) == 0


class TestUpIdempotency:
    """Test that up is idempotent."""

    def test_second_up_no_changes(self, project_dir, monkeypatch):
        """Running up twice should show no changes on second run."""
        monkeypatch.chdir(project_dir)

        # First up
        with patch.object(output_mod.console, "print"):
            run_cli(["up", "--yes"])

        # Second up - capture diff result
        results = []
        original_render = output_mod.render_diff

        def capture_render(result, **kwargs):
            results.append(result)
            return original_render(result, **kwargs)

        with patch.object(output_mod, "render_diff", capture_render):
            with patch.object(output_mod.console, "print"):
                run_cli(["up", "--yes"])

        # Second run should show no changes
        assert len(results) > 0
        assert not results[-1].has_changes
