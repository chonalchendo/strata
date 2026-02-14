"""Tests for schema evolution detection and migration action determination."""

import pyarrow as pa
import pytest

import strata.schema_evolution as evo


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def base_schema():
    """A base schema with common columns."""
    return pa.schema(
        [
            pa.field("user_id", pa.string()),
            pa.field("amount", pa.float64()),
            pa.field("event_ts", pa.timestamp("us")),
        ]
    )


# ---------------------------------------------------------------------------
# No changes
# ---------------------------------------------------------------------------


class TestNoChanges:
    def test_same_schema_has_no_changes(self, base_schema):
        """Identical schemas should produce no changes."""
        result = evo.detect_schema_changes(base_schema, base_schema)

        assert result.changes == []
        assert result.requires_backfill is False
        assert result.migration_action == evo.MigrationAction.NONE

    def test_new_table_has_no_changes(self, base_schema):
        """New table (old_schema=None) should produce no changes."""
        result = evo.detect_schema_changes(None, base_schema)

        assert result.changes == []
        assert result.requires_backfill is False
        assert result.migration_action == evo.MigrationAction.NONE


# ---------------------------------------------------------------------------
# Column added
# ---------------------------------------------------------------------------


class TestColumnAdded:
    def test_column_added_requires_backfill(self, base_schema):
        """Adding a column should trigger full backfill."""
        new_schema = pa.schema(
            [
                pa.field("user_id", pa.string()),
                pa.field("amount", pa.float64()),
                pa.field("event_ts", pa.timestamp("us")),
                pa.field("category", pa.string()),
            ]
        )
        result = evo.detect_schema_changes(base_schema, new_schema)

        assert len(result.changes) == 1
        assert (
            result.changes[0].change_type == evo.SchemaChangeType.COLUMN_ADDED
        )
        assert result.changes[0].column_name == "category"
        assert result.requires_backfill is True
        assert result.migration_action == evo.MigrationAction.FULL_BACKFILL


# ---------------------------------------------------------------------------
# Column removed
# ---------------------------------------------------------------------------


class TestColumnRemoved:
    def test_column_removed_continues_incremental(self, base_schema):
        """Removing a column should continue incremental (column just absent)."""
        new_schema = pa.schema(
            [
                pa.field("user_id", pa.string()),
                pa.field("event_ts", pa.timestamp("us")),
            ]
        )
        result = evo.detect_schema_changes(base_schema, new_schema)

        assert len(result.changes) == 1
        assert (
            result.changes[0].change_type == evo.SchemaChangeType.COLUMN_REMOVED
        )
        assert result.changes[0].column_name == "amount"
        assert result.requires_backfill is False
        assert (
            result.migration_action == evo.MigrationAction.CONTINUE_INCREMENTAL
        )


# ---------------------------------------------------------------------------
# Type widened
# ---------------------------------------------------------------------------


class TestTypeWidened:
    def test_int32_to_int64_is_widening(self):
        """int32 -> int64 is widening, continue incremental."""
        old = pa.schema([pa.field("count", pa.int32())])
        new = pa.schema([pa.field("count", pa.int64())])
        result = evo.detect_schema_changes(old, new)

        assert len(result.changes) == 1
        assert (
            result.changes[0].change_type == evo.SchemaChangeType.TYPE_WIDENED
        )
        assert result.changes[0].column_name == "count"
        assert result.requires_backfill is False
        assert (
            result.migration_action == evo.MigrationAction.CONTINUE_INCREMENTAL
        )

    def test_float32_to_float64_is_widening(self):
        """float32 -> float64 is widening, continue incremental."""
        old = pa.schema([pa.field("score", pa.float32())])
        new = pa.schema([pa.field("score", pa.float64())])
        result = evo.detect_schema_changes(old, new)

        assert len(result.changes) == 1
        assert (
            result.changes[0].change_type == evo.SchemaChangeType.TYPE_WIDENED
        )
        assert result.requires_backfill is False

    def test_int_to_float_is_widening(self):
        """int32 -> float64 is widening (int fits in float)."""
        old = pa.schema([pa.field("value", pa.int32())])
        new = pa.schema([pa.field("value", pa.float64())])
        result = evo.detect_schema_changes(old, new)

        assert len(result.changes) == 1
        assert (
            result.changes[0].change_type == evo.SchemaChangeType.TYPE_WIDENED
        )


# ---------------------------------------------------------------------------
# Type narrowed
# ---------------------------------------------------------------------------


class TestTypeNarrowed:
    def test_int64_to_int32_is_narrowing(self):
        """int64 -> int32 is narrowing, requires backfill."""
        old = pa.schema([pa.field("count", pa.int64())])
        new = pa.schema([pa.field("count", pa.int32())])
        result = evo.detect_schema_changes(old, new)

        assert len(result.changes) == 1
        assert (
            result.changes[0].change_type == evo.SchemaChangeType.TYPE_NARROWED
        )
        assert result.changes[0].column_name == "count"
        assert result.requires_backfill is True
        assert result.migration_action == evo.MigrationAction.FULL_BACKFILL

    def test_float_to_int_is_narrowing(self):
        """float64 -> int32 is narrowing (potential precision loss)."""
        old = pa.schema([pa.field("value", pa.float64())])
        new = pa.schema([pa.field("value", pa.int32())])
        result = evo.detect_schema_changes(old, new)

        assert len(result.changes) == 1
        assert (
            result.changes[0].change_type == evo.SchemaChangeType.TYPE_NARROWED
        )
        assert result.requires_backfill is True

    def test_cross_family_is_narrowing(self):
        """string -> int64 is treated as narrowing (safe default)."""
        old = pa.schema([pa.field("val", pa.string())])
        new = pa.schema([pa.field("val", pa.int64())])
        result = evo.detect_schema_changes(old, new)

        assert len(result.changes) == 1
        assert (
            result.changes[0].change_type == evo.SchemaChangeType.TYPE_NARROWED
        )
        assert result.requires_backfill is True


# ---------------------------------------------------------------------------
# Multiple changes
# ---------------------------------------------------------------------------


class TestMultipleChanges:
    def test_mixed_changes_backfill_wins(self):
        """When multiple changes include backfill, overall action is backfill."""
        old = pa.schema(
            [
                pa.field("user_id", pa.string()),
                pa.field("count", pa.int32()),
                pa.field("old_col", pa.string()),
            ]
        )
        new = pa.schema(
            [
                pa.field("user_id", pa.string()),
                pa.field("count", pa.int64()),  # widened (incremental)
                pa.field("new_col", pa.float64()),  # added (backfill)
                # old_col removed (incremental)
            ]
        )
        result = evo.detect_schema_changes(old, new)

        assert len(result.changes) == 3
        assert result.requires_backfill is True
        assert result.migration_action == evo.MigrationAction.FULL_BACKFILL

        # Verify individual change types
        change_types = {c.column_name: c.change_type for c in result.changes}
        assert change_types["old_col"] == evo.SchemaChangeType.COLUMN_REMOVED
        assert change_types["new_col"] == evo.SchemaChangeType.COLUMN_ADDED
        assert change_types["count"] == evo.SchemaChangeType.TYPE_WIDENED

    def test_only_incremental_changes(self):
        """Widened + removed should be incremental overall."""
        old = pa.schema(
            [
                pa.field("user_id", pa.string()),
                pa.field("count", pa.int32()),
                pa.field("old_col", pa.string()),
            ]
        )
        new = pa.schema(
            [
                pa.field("user_id", pa.string()),
                pa.field("count", pa.int64()),  # widened
                # old_col removed
            ]
        )
        result = evo.detect_schema_changes(old, new)

        assert len(result.changes) == 2
        assert result.requires_backfill is False
        assert (
            result.migration_action == evo.MigrationAction.CONTINUE_INCREMENTAL
        )
