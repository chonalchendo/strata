"""Tests for SQLite registry implementation."""

from __future__ import annotations

import strata.backends.sqlite as sqlite
import strata.registry as registry


class TestSqliteRegistryInitialize:
    """Tests for SqliteRegistry.initialize()."""

    def test_initialize_creates_tables(self, tmp_path):
        """Initialize creates objects, changelog, and meta tables."""
        db_path = tmp_path / "test.db"
        reg = sqlite.SqliteRegistry(path=str(db_path))
        reg.initialize()

        # Verify tables exist by querying them
        import sqlite3

        conn = sqlite3.connect(str(db_path))
        cursor = conn.cursor()

        # Check objects table
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='objects'"
        )
        assert cursor.fetchone() is not None

        # Check changelog table
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='changelog'"
        )
        assert cursor.fetchone() is not None

        # Check meta table
        cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='meta'"
        )
        assert cursor.fetchone() is not None

        conn.close()

    def test_initialize_sets_initial_meta(self, tmp_path):
        """Initialize sets lineage, serial, and strata_version."""
        db_path = tmp_path / "test.db"
        reg = sqlite.SqliteRegistry(path=str(db_path))
        reg.initialize()

        # Check initial metadata
        assert reg.get_meta("lineage") is not None
        assert reg.get_meta("serial") == "0"
        assert reg.get_meta("strata_version") == "0.1.0"

    def test_initialize_is_idempotent(self, tmp_path):
        """Initialize can be called multiple times safely."""
        db_path = tmp_path / "test.db"
        reg = sqlite.SqliteRegistry(path=str(db_path))
        reg.initialize()
        lineage1 = reg.get_meta("lineage")

        # Second initialization should not change lineage
        reg.initialize()
        lineage2 = reg.get_meta("lineage")

        assert lineage1 == lineage2


class TestSqliteRegistryObjects:
    """Tests for SqliteRegistry object CRUD operations."""

    def test_put_and_get_object(self, tmp_path):
        """Can put and retrieve an object."""
        db_path = tmp_path / "test.db"
        reg = sqlite.SqliteRegistry(path=str(db_path))
        reg.initialize()

        obj = registry.ObjectRecord(
            kind="entity",
            name="user",
            spec_hash="abc123",
            spec_json='{"name": "user", "join_keys": ["user_id"]}',
            version=1,
        )
        reg.put_object(obj, applied_by="test@host")

        result = reg.get_object("entity", "user")
        assert result is not None
        assert result.kind == "entity"
        assert result.name == "user"
        assert result.spec_hash == "abc123"
        assert result.version == 1

    def test_get_object_not_found(self, tmp_path):
        """Returns None for non-existent object."""
        db_path = tmp_path / "test.db"
        reg = sqlite.SqliteRegistry(path=str(db_path))
        reg.initialize()

        result = reg.get_object("entity", "nonexistent")
        assert result is None

    def test_list_objects_all(self, tmp_path):
        """List returns all objects."""
        db_path = tmp_path / "test.db"
        reg = sqlite.SqliteRegistry(path=str(db_path))
        reg.initialize()

        obj1 = registry.ObjectRecord(
            kind="entity", name="user", spec_hash="a", spec_json="{}", version=1
        )
        obj2 = registry.ObjectRecord(
            kind="feature_table",
            name="features",
            spec_hash="b",
            spec_json="{}",
            version=1,
        )
        reg.put_object(obj1, applied_by="test@host")
        reg.put_object(obj2, applied_by="test@host")

        result = reg.list_objects()
        assert len(result) == 2

    def test_list_objects_by_kind(self, tmp_path):
        """List can filter by kind."""
        db_path = tmp_path / "test.db"
        reg = sqlite.SqliteRegistry(path=str(db_path))
        reg.initialize()

        obj1 = registry.ObjectRecord(
            kind="entity", name="user", spec_hash="a", spec_json="{}", version=1
        )
        obj2 = registry.ObjectRecord(
            kind="feature_table",
            name="features",
            spec_hash="b",
            spec_json="{}",
            version=1,
        )
        obj3 = registry.ObjectRecord(
            kind="entity", name="product", spec_hash="c", spec_json="{}", version=1
        )
        reg.put_object(obj1, applied_by="test@host")
        reg.put_object(obj2, applied_by="test@host")
        reg.put_object(obj3, applied_by="test@host")

        result = reg.list_objects(kind="entity")
        assert len(result) == 2
        assert all(obj.kind == "entity" for obj in result)

    def test_put_object_updates_version(self, tmp_path):
        """Updating an object increments version."""
        db_path = tmp_path / "test.db"
        reg = sqlite.SqliteRegistry(path=str(db_path))
        reg.initialize()

        obj1 = registry.ObjectRecord(
            kind="entity", name="user", spec_hash="v1", spec_json='{"v": 1}', version=1
        )
        reg.put_object(obj1, applied_by="test@host")

        # Update with new hash
        obj2 = registry.ObjectRecord(
            kind="entity", name="user", spec_hash="v2", spec_json='{"v": 2}', version=1
        )
        reg.put_object(obj2, applied_by="test@host")

        result = reg.get_object("entity", "user")
        assert result is not None
        assert result.version == 2
        assert result.spec_hash == "v2"

    def test_delete_object(self, tmp_path):
        """Can delete an object."""
        db_path = tmp_path / "test.db"
        reg = sqlite.SqliteRegistry(path=str(db_path))
        reg.initialize()

        obj = registry.ObjectRecord(
            kind="entity", name="user", spec_hash="abc", spec_json="{}", version=1
        )
        reg.put_object(obj, applied_by="test@host")
        assert reg.get_object("entity", "user") is not None

        reg.delete_object("entity", "user", applied_by="test@host")
        assert reg.get_object("entity", "user") is None

    def test_delete_nonexistent_is_noop(self, tmp_path):
        """Deleting non-existent object does nothing."""
        db_path = tmp_path / "test.db"
        reg = sqlite.SqliteRegistry(path=str(db_path))
        reg.initialize()

        # Should not raise
        reg.delete_object("entity", "nonexistent", applied_by="test@host")


class TestSqliteRegistryChangelog:
    """Tests for SqliteRegistry changelog tracking."""

    def test_changelog_tracks_create(self, tmp_path):
        """Create operation is logged."""
        db_path = tmp_path / "test.db"
        reg = sqlite.SqliteRegistry(path=str(db_path))
        reg.initialize()

        obj = registry.ObjectRecord(
            kind="entity", name="user", spec_hash="abc", spec_json="{}", version=1
        )
        reg.put_object(obj, applied_by="test@host")

        changelog = reg.get_changelog()
        assert len(changelog) == 1
        assert changelog[0].operation == "create"
        assert changelog[0].kind == "entity"
        assert changelog[0].name == "user"
        assert changelog[0].old_hash is None
        assert changelog[0].new_hash == "abc"

    def test_changelog_tracks_update(self, tmp_path):
        """Update operation is logged with old and new hash."""
        db_path = tmp_path / "test.db"
        reg = sqlite.SqliteRegistry(path=str(db_path))
        reg.initialize()

        obj1 = registry.ObjectRecord(
            kind="entity", name="user", spec_hash="v1", spec_json="{}", version=1
        )
        reg.put_object(obj1, applied_by="test@host")

        obj2 = registry.ObjectRecord(
            kind="entity", name="user", spec_hash="v2", spec_json="{}", version=1
        )
        reg.put_object(obj2, applied_by="test@host")

        changelog = reg.get_changelog()
        assert len(changelog) == 2
        # Most recent first
        assert changelog[0].operation == "update"
        assert changelog[0].old_hash == "v1"
        assert changelog[0].new_hash == "v2"

    def test_changelog_tracks_delete(self, tmp_path):
        """Delete operation is logged."""
        db_path = tmp_path / "test.db"
        reg = sqlite.SqliteRegistry(path=str(db_path))
        reg.initialize()

        obj = registry.ObjectRecord(
            kind="entity", name="user", spec_hash="abc", spec_json="{}", version=1
        )
        reg.put_object(obj, applied_by="test@host")
        reg.delete_object("entity", "user", applied_by="test@host")

        changelog = reg.get_changelog()
        assert len(changelog) == 2
        # Most recent first
        assert changelog[0].operation == "delete"
        assert changelog[0].old_hash == "abc"
        assert changelog[0].new_hash is None

    def test_changelog_respects_limit(self, tmp_path):
        """Changelog respects limit parameter."""
        db_path = tmp_path / "test.db"
        reg = sqlite.SqliteRegistry(path=str(db_path))
        reg.initialize()

        # Create 5 objects
        for i in range(5):
            obj = registry.ObjectRecord(
                kind="entity",
                name=f"user{i}",
                spec_hash=f"h{i}",
                spec_json="{}",
                version=1,
            )
            reg.put_object(obj, applied_by="test@host")

        changelog = reg.get_changelog(limit=3)
        assert len(changelog) == 3


class TestSqliteRegistryMeta:
    """Tests for SqliteRegistry metadata operations."""

    def test_meta_get_set(self, tmp_path):
        """Can get and set metadata values."""
        db_path = tmp_path / "test.db"
        reg = sqlite.SqliteRegistry(path=str(db_path))
        reg.initialize()

        reg.set_meta("custom_key", "custom_value")
        assert reg.get_meta("custom_key") == "custom_value"

    def test_meta_get_nonexistent(self, tmp_path):
        """Returns None for non-existent metadata key."""
        db_path = tmp_path / "test.db"
        reg = sqlite.SqliteRegistry(path=str(db_path))
        reg.initialize()

        assert reg.get_meta("nonexistent") is None

    def test_meta_set_overwrites(self, tmp_path):
        """Setting metadata overwrites existing value."""
        db_path = tmp_path / "test.db"
        reg = sqlite.SqliteRegistry(path=str(db_path))
        reg.initialize()

        reg.set_meta("key", "value1")
        reg.set_meta("key", "value2")
        assert reg.get_meta("key") == "value2"

    def test_serial_increments_on_put(self, tmp_path):
        """Serial increments on each object mutation."""
        db_path = tmp_path / "test.db"
        reg = sqlite.SqliteRegistry(path=str(db_path))
        reg.initialize()

        assert reg.get_meta("serial") == "0"

        obj = registry.ObjectRecord(
            kind="entity", name="user", spec_hash="abc", spec_json="{}", version=1
        )
        reg.put_object(obj, applied_by="test@host")
        assert reg.get_meta("serial") == "1"

        reg.delete_object("entity", "user", applied_by="test@host")
        assert reg.get_meta("serial") == "2"
