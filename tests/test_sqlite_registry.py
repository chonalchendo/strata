"""Tests for SQLite registry implementation."""

from __future__ import annotations

import time
from datetime import datetime, timezone

import strata.infra.backends.sqlite as sqlite
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
            kind="entity",
            name="product",
            spec_hash="c",
            spec_json="{}",
            version=1,
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
            kind="entity",
            name="user",
            spec_hash="v1",
            spec_json='{"v": 1}',
            version=1,
        )
        reg.put_object(obj1, applied_by="test@host")

        # Update with new hash
        obj2 = registry.ObjectRecord(
            kind="entity",
            name="user",
            spec_hash="v2",
            spec_json='{"v": 2}',
            version=1,
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
            kind="entity",
            name="user",
            spec_hash="abc",
            spec_json="{}",
            version=1,
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
            kind="entity",
            name="user",
            spec_hash="abc",
            spec_json="{}",
            version=1,
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
            kind="entity",
            name="user",
            spec_hash="v1",
            spec_json="{}",
            version=1,
        )
        reg.put_object(obj1, applied_by="test@host")

        obj2 = registry.ObjectRecord(
            kind="entity",
            name="user",
            spec_hash="v2",
            spec_json="{}",
            version=1,
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
            kind="entity",
            name="user",
            spec_hash="abc",
            spec_json="{}",
            version=1,
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
            kind="entity",
            name="user",
            spec_hash="abc",
            spec_json="{}",
            version=1,
        )
        reg.put_object(obj, applied_by="test@host")
        assert reg.get_meta("serial") == "1"

        reg.delete_object("entity", "user", applied_by="test@host")
        assert reg.get_meta("serial") == "2"


class TestSqliteRegistryQualityResults:
    """Tests for SqliteRegistry quality result persistence."""

    def test_put_and_get_quality_result(self, tmp_path):
        """Can store and retrieve a quality result."""
        db_path = tmp_path / "test.db"
        reg = sqlite.SqliteRegistry(path=str(db_path))
        reg.initialize()

        now = datetime.now(timezone.utc)
        result = registry.QualityResultRecord(
            id=None,
            timestamp=now,
            table_name="user_features",
            passed=True,
            has_warnings=False,
            rows_checked=1000,
            results_json='{"fields": []}',
            build_id=None,
        )
        reg.put_quality_result(result)

        results = reg.get_quality_results("user_features")
        assert len(results) == 1
        assert results[0].id is not None
        assert results[0].table_name == "user_features"
        assert results[0].passed is True
        assert results[0].has_warnings is False
        assert results[0].rows_checked == 1000
        assert results[0].results_json == '{"fields": []}'

    def test_get_quality_results_ordering(self, tmp_path):
        """Quality results are returned newest first."""
        db_path = tmp_path / "test.db"
        reg = sqlite.SqliteRegistry(path=str(db_path))
        reg.initialize()

        t1 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        t2 = datetime(2026, 1, 2, 12, 0, 0, tzinfo=timezone.utc)
        t3 = datetime(2026, 1, 3, 12, 0, 0, tzinfo=timezone.utc)

        for t, passed in [(t1, True), (t2, False), (t3, True)]:
            reg.put_quality_result(
                registry.QualityResultRecord(
                    id=None,
                    timestamp=t,
                    table_name="user_features",
                    passed=passed,
                    has_warnings=False,
                    rows_checked=100,
                    results_json="{}",
                )
            )

        results = reg.get_quality_results("user_features")
        assert len(results) == 3
        # Most recent first
        assert results[0].timestamp == t3
        assert results[1].timestamp == t2
        assert results[2].timestamp == t1

    def test_get_quality_results_limit(self, tmp_path):
        """Quality results respect the limit parameter."""
        db_path = tmp_path / "test.db"
        reg = sqlite.SqliteRegistry(path=str(db_path))
        reg.initialize()

        for i in range(5):
            reg.put_quality_result(
                registry.QualityResultRecord(
                    id=None,
                    timestamp=datetime(2026, 1, i + 1, tzinfo=timezone.utc),
                    table_name="user_features",
                    passed=True,
                    has_warnings=False,
                    rows_checked=100,
                    results_json="{}",
                )
            )

        results = reg.get_quality_results("user_features", limit=2)
        assert len(results) == 2

    def test_quality_result_with_build_id(self, tmp_path):
        """Quality results can reference a build record."""
        db_path = tmp_path / "test.db"
        reg = sqlite.SqliteRegistry(path=str(db_path))
        reg.initialize()

        now = datetime.now(timezone.utc)
        result = registry.QualityResultRecord(
            id=None,
            timestamp=now,
            table_name="user_features",
            passed=True,
            has_warnings=True,
            rows_checked=500,
            results_json='{"warnings": 2}',
            build_id=42,
        )
        reg.put_quality_result(result)

        results = reg.get_quality_results("user_features")
        assert results[0].build_id == 42
        assert results[0].has_warnings is True


class TestSqliteRegistryBuildRecords:
    """Tests for SqliteRegistry build record persistence."""

    def test_put_and_get_build_record(self, tmp_path):
        """Can store and retrieve a build record."""
        db_path = tmp_path / "test.db"
        reg = sqlite.SqliteRegistry(path=str(db_path))
        reg.initialize()

        now = datetime.now(timezone.utc)
        record = registry.BuildRecord(
            id=None,
            timestamp=now,
            table_name="user_features",
            status="success",
            row_count=1000,
            duration_ms=1234.5,
            data_timestamp_max="2026-01-15T00:00:00Z",
        )
        reg.put_build_record(record)

        result = reg.get_latest_build("user_features")
        assert result is not None
        assert result.id is not None
        assert result.table_name == "user_features"
        assert result.status == "success"
        assert result.row_count == 1000
        assert result.duration_ms == 1234.5
        assert result.data_timestamp_max == "2026-01-15T00:00:00Z"

    def test_get_latest_build(self, tmp_path):
        """Returns the most recent build record."""
        db_path = tmp_path / "test.db"
        reg = sqlite.SqliteRegistry(path=str(db_path))
        reg.initialize()

        t1 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        t2 = datetime(2026, 1, 2, 12, 0, 0, tzinfo=timezone.utc)

        for t, status in [(t1, "success"), (t2, "failed")]:
            reg.put_build_record(
                registry.BuildRecord(
                    id=None,
                    timestamp=t,
                    table_name="user_features",
                    status=status,
                )
            )

        latest = reg.get_latest_build("user_features")
        assert latest is not None
        assert latest.status == "failed"
        assert latest.timestamp == t2

    def test_get_latest_build_none(self, tmp_path):
        """Returns None when no builds exist for a table."""
        db_path = tmp_path / "test.db"
        reg = sqlite.SqliteRegistry(path=str(db_path))
        reg.initialize()

        result = reg.get_latest_build("nonexistent_table")
        assert result is None

    def test_get_build_records_by_table(self, tmp_path):
        """Build records can be filtered by table name."""
        db_path = tmp_path / "test.db"
        reg = sqlite.SqliteRegistry(path=str(db_path))
        reg.initialize()

        now = datetime.now(timezone.utc)
        for table in ["table_a", "table_b", "table_a"]:
            reg.put_build_record(
                registry.BuildRecord(
                    id=None,
                    timestamp=now,
                    table_name=table,
                    status="success",
                )
            )
            # Small delay to ensure distinct timestamps
            time.sleep(0.01)

        results_a = reg.get_build_records(table_name="table_a")
        assert len(results_a) == 2
        assert all(r.table_name == "table_a" for r in results_a)

        results_b = reg.get_build_records(table_name="table_b")
        assert len(results_b) == 1

    def test_get_build_records_all(self, tmp_path):
        """Build records can be retrieved without table filter."""
        db_path = tmp_path / "test.db"
        reg = sqlite.SqliteRegistry(path=str(db_path))
        reg.initialize()

        now = datetime.now(timezone.utc)
        for table in ["table_a", "table_b", "table_c"]:
            reg.put_build_record(
                registry.BuildRecord(
                    id=None,
                    timestamp=now,
                    table_name=table,
                    status="success",
                )
            )

        all_records = reg.get_build_records()
        assert len(all_records) == 3

    def test_get_build_records_limit(self, tmp_path):
        """Build records respect the limit parameter."""
        db_path = tmp_path / "test.db"
        reg = sqlite.SqliteRegistry(path=str(db_path))
        reg.initialize()

        for i in range(5):
            reg.put_build_record(
                registry.BuildRecord(
                    id=None,
                    timestamp=datetime(2026, 1, i + 1, tzinfo=timezone.utc),
                    table_name="user_features",
                    status="success",
                )
            )

        results = reg.get_build_records(limit=3)
        assert len(results) == 3

    def test_build_record_optional_fields(self, tmp_path):
        """Build records work with only required fields."""
        db_path = tmp_path / "test.db"
        reg = sqlite.SqliteRegistry(path=str(db_path))
        reg.initialize()

        now = datetime.now(timezone.utc)
        record = registry.BuildRecord(
            id=None,
            timestamp=now,
            table_name="user_features",
            status="skipped",
        )
        reg.put_build_record(record)

        result = reg.get_latest_build("user_features")
        assert result is not None
        assert result.status == "skipped"
        assert result.row_count is None
        assert result.duration_ms is None
        assert result.data_timestamp_max is None


class TestSqliteRegistryAutoInitBuildTables:
    """Tests for auto-initialization of build tables without full initialize()."""

    def test_put_quality_result_without_initialize(self, tmp_path):
        """put_quality_result auto-creates quality_results table."""
        db_path = tmp_path / "registry.db"
        reg = sqlite.SqliteRegistry(path=str(db_path))
        # No reg.initialize() — simulates 'build' without 'up'

        now = datetime.now(timezone.utc)
        result = registry.QualityResultRecord(
            id=None,
            timestamp=now,
            table_name="user_features",
            passed=True,
            has_warnings=False,
            rows_checked=500,
            results_json='{"fields": []}',
        )
        reg.put_quality_result(result)

        results = reg.get_quality_results("user_features")
        assert len(results) == 1
        assert results[0].table_name == "user_features"
        assert results[0].passed is True

    def test_put_build_record_without_initialize(self, tmp_path):
        """put_build_record auto-creates build_records table."""
        db_path = tmp_path / "registry.db"
        reg = sqlite.SqliteRegistry(path=str(db_path))
        # No reg.initialize()

        now = datetime.now(timezone.utc)
        record = registry.BuildRecord(
            id=None,
            timestamp=now,
            table_name="user_features",
            status="success",
            row_count=500,
            duration_ms=100.0,
        )
        reg.put_build_record(record)

        result = reg.get_latest_build("user_features")
        assert result is not None
        assert result.table_name == "user_features"
        assert result.status == "success"

    def test_auto_init_creates_parent_directory(self, tmp_path):
        """Auto-init creates missing parent directories."""
        db_path = tmp_path / "nested" / "dir" / "registry.db"
        reg = sqlite.SqliteRegistry(path=str(db_path))

        now = datetime.now(timezone.utc)
        reg.put_build_record(
            registry.BuildRecord(
                id=None,
                timestamp=now,
                table_name="t",
                status="success",
            )
        )

        assert db_path.exists()

    def test_auto_init_is_idempotent_with_initialize(self, tmp_path):
        """Auto-init doesn't break a fully initialized registry."""
        db_path = tmp_path / "registry.db"
        reg = sqlite.SqliteRegistry(path=str(db_path))
        reg.initialize()  # Full init first

        now = datetime.now(timezone.utc)
        reg.put_build_record(
            registry.BuildRecord(
                id=None,
                timestamp=now,
                table_name="t",
                status="success",
            )
        )

        result = reg.get_latest_build("t")
        assert result is not None
        assert result.status == "success"
